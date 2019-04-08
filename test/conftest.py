import subprocess
import time
import pytest


def compose_up():
    subprocess.call(["docker-compose", "up", "-d"])
    time.sleep(20)


def compose_down():
    subprocess.call(["docker-compose", "down"])
    subprocess.call(["docker-compose", "rm"])


@pytest.fixture(scope='session', autouse=True)
def scope_class():
    # compose_up()
    yield
    # compose_down()
