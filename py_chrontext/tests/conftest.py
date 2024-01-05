# Adapted from https://github.com/yehoshuadimarsky/bcpandas/blob/master/bcpandas/tests/conftest.py and utils.py
# License:
#
# MIT License
#
# Copyright (c) 2019-2020 yehoshuadimarsky
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import time

import docker
import pytest
import pathlib

PATH_HERE = pathlib.Path(__file__).parent
print(PATH_HERE)
OXIGRAPH_SERVER_IMAGE = "oxigraph/oxigraph:v0.3.8"
OXIGRAPH_CONTAINER_NAME ="my-oxigraph-server"


@pytest.fixture(scope="module")
def oxigraph_db():
    client = docker.from_env()
    try:
        existing = client.containers.get(OXIGRAPH_CONTAINER_NAME)
        existing.stop()
        existing.remove()
    except:
        pass

    container = client.containers.run(
        image=OXIGRAPH_SERVER_IMAGE,
        name=OXIGRAPH_CONTAINER_NAME,
        detach=True,
        ports={"7878/tcp": "7878/tcp"},
        command=[
            "--location",
            "/data",
            "serve",
            "--bind",
            "0.0.0.0:7878",
        ]
    )
    time.sleep(20)
    yield
    print("Stopping container")
    container.stop()
    print("Deleting container")
    container.remove()
    print("all done!")