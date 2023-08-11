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
import requests


PATH_HERE = pathlib.Path(__file__).parent
print(PATH_HERE)
OXIGRAPH_SERVER_IMAGE = "oxigraph/oxigraph:v0.3.8"
DREMIO_DOCKER_PATH = PATH_HERE / "dremio_docker"
DREMIO_CONTAINER_NAME = "my-dremio-server"
DREMIO_ORIGIN = "http://127.0.0.1:9047"
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

@pytest.fixture(scope="module")
def dremio_db():
    client = docker.from_env()
    try:
        existing = client.containers.get(DREMIO_CONTAINER_NAME)
        existing.stop()
        existing.remove()
    except:
        pass

    (image, logs) = client.images.build(
        path=str(DREMIO_DOCKER_PATH.absolute()),
    )
    print(image)
    for l in logs:
        print(l)
    container = client.containers.run(
        image=image,
        name=DREMIO_CONTAINER_NAME,
        detach=True,
        ports={"9047/tcp": "9047",
               "32010/tcp": "32010",
               "45678/tcp": "45678"},
    )
    time.sleep(40)
    yield
    print("Stopping container")
    container.stop()
    print("Deleting container")
    container.remove()
    print("all done!")


@pytest.fixture(scope="module")
def dremio_testdata(dremio_db):
    auth_resp = requests.post(
        url=f"{DREMIO_ORIGIN}/apiv2/login",
        headers={"content-type": "application/json"},
        json={"userName": "dremio", "password": "dremio123"}
    )
    assert (auth_resp.ok)
    print(auth_resp.json())
    token = auth_resp.json().get("token")
    bearer_auth = f"Bearer {token}"
    promotion_resp = requests.post(
        headers={
            "content-type": "application/json",
            "authorization": bearer_auth},
        url=f"{DREMIO_ORIGIN}/api/v3/catalog",
        json={
            "entityType": "source",
            "config": {
                "path": "/var/dremio-data"
            },
            "type": "NAS",
            "name": "my_nas",
            "metadataPolicy": {
                "authTTLMs": 86400000,
                "namesRefreshMs": 3600000,
                "datasetRefreshAfterMs": 3600000,
                "datasetExpireAfterMs": 10800000,
                "datasetUpdateMode": "PREFETCH_QUERIED",
                "deleteUnavailableDatasets": True,
                "autoPromoteDatasets": False
            },
            "accelerationGracePeriodMs": 10800000,
            "accelerationRefreshPeriodMs": 3600000,
            "accelerationNeverExpire": False,
            "accelerationNeverRefresh": False
        })
    assert (promotion_resp.ok)
    print(promotion_resp.json())

    file_promotion_resp = requests.post(
        url=f"{DREMIO_ORIGIN}/api/v3/catalog/dremio%3A%2Fmy_nas%2Fts.parquet",
        headers={
            "content-type": "application/json",
            "authorization": bearer_auth},
        json={
            "entityType": "dataset",
            "id": "dremio:/my_nas/ts.parquet",
            "path": [
                "my_nas", "ts.parquet"
            ],
            "type": "PHYSICAL_DATASET",
            "format": {
                "type": "Parquet"
            }
        }
    )
    assert(file_promotion_resp.ok)
    print(file_promotion_resp.json())