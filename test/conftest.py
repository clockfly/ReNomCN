"""
Copyright 2019, Grid.

This source code is licensed under the ReNom Subscription Agreement, version 1.0.
ReNom Subscription Agreement Ver. 1.0 (https://www.renom.jp/info/license/index.html)
"""

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
