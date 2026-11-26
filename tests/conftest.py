"""Fixtures shared by every test file. pytest loads this file automatically.

pki         certificates + tokens for the ids in tests/helpers.py, created once per test run
cloud       a fresh Cloud Service on a free port, stopped after the test
make_robot  factory: start a robot by id; every robot started is shut down after the test
make_user   factory: start a user by name; every user started is shut down after the test
"""
import asyncio

import pytest
import pytest_asyncio

from cloud_service.server import CloudService
from common.config import cloud_url
from common.mesh import free_port
from robot.client import RobotClient
from tests.helpers import HOST, ROBOTS, USERS, wait_until
from tools.setup_security import setup
from user.client import UserClient


@pytest.fixture(scope="session")
def pki(tmp_path_factory):
    folder = tmp_path_factory.mktemp("pki")
    setup(folder, robots=ROBOTS, users=USERS)
    return folder


@pytest_asyncio.fixture
async def cloud(pki):
    svc = CloudService(HOST, free_port(HOST), pki)
    await svc.start()                     # setup
    yield svc                             # the test runs here
    await svc.stop()                      # cleanup (stops any Players), even if the test failed


@pytest_asyncio.fixture
async def make_robot(cloud, pki):
    started = []

    async def start(robot_id="robot-1", sensor=None):
        bot = RobotClient(robot_id, cloud_url(HOST, cloud.port), HOST, pki, sensor)
        task = asyncio.create_task(bot.run())
        started.append((bot, task))
        await wait_until(lambda: robot_id in cloud.robots and cloud.robots[robot_id].state != "offline")
        return bot, task

    yield start
    for bot, task in started:
        task.cancel()
        await bot.shutdown()


@pytest_asyncio.fixture
async def make_user(cloud, pki):
    started = []

    async def start(name="user"):
        u = UserClient(name, cloud_url(HOST, cloud.port), HOST, pki)
        await u.start()
        started.append(u)
        return u

    yield start
    for u in started:
        await u.shutdown()
