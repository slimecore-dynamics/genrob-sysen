"""Create the certificates and tokens every program needs.

Run from the repo root before starting anything:
    python -m tools.setup_security --robots robot-1 robot-2 --users user

Safe to re-run: the CA is created only once; listed robots/users get a fresh certificate and token,
and tokens.json keeps entries for ids not listed. For several machines, add each machine's address:
    python -m tools.setup_security --robots robot-1 --users user --ip 127.0.0.1 192.168.1.20

ASSUMPTION A11: identities are issued ahead of time by whoever runs this script (in a real fleet,
during robot onboarding), and each robot/user only receives its own folder.
Explained in Notebook 3.
"""
import argparse
from pathlib import Path

from common.config import DEFAULT_PKI_DIR
from common.pki import make_ca, make_cert, write_secret
from common.security import TokenStore, check_name, fingerprint, identity_dir, new_token


def setup(pki_dir, robots=(), users=(), ips=("127.0.0.1",)) -> None:
    for name in (*robots, *users):
        check_name(name)                 # before writing anything
    pki_dir = Path(pki_dir)
    ca_dir = pki_dir / "ca"
    if not (ca_dir / "cert.pem").is_file():
        make_ca(ca_dir)
        print(f"created CA            {ca_dir}")

    for kind in ("cloud", "player"):
        make_cert(ca_dir, identity_dir(pki_dir, kind), kind, ips)
        print(f"created certificate   {identity_dir(pki_dir, kind)}")

    tokens_path = pki_dir / "tokens.json"
    store = TokenStore.load(tokens_path) if tokens_path.is_file() else TokenStore()

    for kind, names, table in (("robots", robots, store.robots), ("users", users, store.users)):
        for name in names:
            folder = make_cert(ca_dir, identity_dir(pki_dir, kind, name), name, ips)
            token = new_token()
            write_secret(folder / "token", token.encode())
            table[name] = fingerprint(token)          # the Cloud keeps only the fingerprint
            print(f"created identity      {folder}  (cert + token)")

    store.save(tokens_path)
    print(f"updated               {tokens_path}")


def name_arg(text: str) -> str:
    try:
        return check_name(text)
    except ValueError as err:
        raise argparse.ArgumentTypeError(str(err)) from None


def main() -> None:
    parser = argparse.ArgumentParser(description="Create CA, certificates and tokens")
    parser.add_argument("--pki-dir", type=Path, default=DEFAULT_PKI_DIR)
    parser.add_argument("--robots", nargs="*", default=[], metavar="ROBOT_ID", type=name_arg)
    parser.add_argument("--users", nargs="*", default=[], metavar="NAME", type=name_arg)
    parser.add_argument("--ip", nargs="+", default=["127.0.0.1"], help="addresses the certificates are valid for")
    args = parser.parse_args()
    if not args.robots and not args.users:
        parser.error("give at least one --robots or --users id")
    setup(args.pki_dir, args.robots, args.users, args.ip)
    print("\nKeep pki/ca/key.pem private: it can create any identity. pki/ is in .gitignore.")


if __name__ == "__main__":
    main()
