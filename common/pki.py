"""Create a private Certificate Authority and certificates signed by it.

Used by: tools/setup_security.py, and the test suite's fixtures.
Explained in Notebook 3, section 3.

Each certificate is valid for the given IP addresses plus "localhost". To run across several
machines, include every machine's address (setup_security --ip ...).
"""
import datetime
import ipaddress
import os
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

CA_DAYS = 365
CERT_DAYS = 90


def _new_key():
    return ec.generate_private_key(ec.SECP256R1())


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def write_secret(path: Path, data: bytes) -> None:
    """Write a private key or token readable only by its owner (mode 600 on Linux and macOS;
    Windows ignores the mode, and the file inherits the folder's permissions there)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.chmod(path, 0o600)      # os.open's mode only applies to a new file; this covers an old one


def _save(folder: Path, key, cert) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    write_secret(folder / "key.pem", key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    (folder / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return folder


def make_ca(folder, name: str = "Robot System CA") -> Path:
    """Self-signed CA. Its key.pem can sign any identity: keep it off the Cloud Service and robots."""
    key = _new_key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    cert = (x509.CertificateBuilder()
            .subject_name(subject).issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(_now() - datetime.timedelta(minutes=5))
            .not_valid_after(_now() + datetime.timedelta(days=CA_DAYS))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                         content_commitment=False, key_encipherment=False,
                                         data_encipherment=False, key_agreement=False,
                                         encipher_only=False, decipher_only=False), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .sign(key, hashes.SHA256()))
    return _save(Path(folder), key, cert)


def make_cert(ca_folder, folder, common_name: str, ips=("127.0.0.1",)) -> Path:
    """Certificate for one program, signed by the CA, usable for both server and client roles."""
    ca_folder = Path(ca_folder)
    ca_key = serialization.load_pem_private_key((ca_folder / "key.pem").read_bytes(), None)
    ca_cert = x509.load_pem_x509_certificate((ca_folder / "cert.pem").read_bytes())
    key = _new_key()
    names = [x509.IPAddress(ipaddress.ip_address(ip)) for ip in ips] + [x509.DNSName("localhost")]
    cert = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(_now() - datetime.timedelta(minutes=5))
            .not_valid_after(_now() + datetime.timedelta(days=CERT_DAYS))
            .add_extension(x509.SubjectAlternativeName(names), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
                           critical=False)
            .sign(ca_key, hashes.SHA256()))
    return _save(Path(folder), key, cert)
