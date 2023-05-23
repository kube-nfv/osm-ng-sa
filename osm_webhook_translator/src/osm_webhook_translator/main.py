#######################################################################################
# Copyright ETSI Contributors and Others.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#######################################################################################
from datetime import datetime
import logging
import os
from random import randint

from fastapi import FastAPI
import requests


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
app = FastAPI()


def send_to_airflow(output_endpoint, content):
    try:
        requests.Session()
        # Airflow params should come from env variables from configmaps and secrets
        airflow_host = os.environ["AIRFLOW_HOST"]
        airflow_port = os.environ["AIRFLOW_PORT"]
        airflow_user = os.environ["AIRFLOW_USER"]
        airflow_pass = os.environ["AIRFLOW_PASS"]
        url = f"http://{airflow_host}:{airflow_port}/api/v1/dags/{output_endpoint}/dagRuns"
        rnd = str(randint(0, 999999)).rjust(6, "0")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        dag_run_id = output_endpoint + "_" + timestamp + "_" + rnd
        logger.info(f"HTTP POST {url}")
        req = requests.post(
            url=url,
            auth=(airflow_user, airflow_pass),
            json={"dag_run_id": dag_run_id, "conf": content},
        )
        logger.info(f"Response: {req.text}")
        # timeout and retries
    except Exception as e:
        logger.error(f"HTTP error: {repr(e)}")
        raise requests.HTTPError(status_code=403, detail=repr(e))


@app.post("/{input_endpoint}")
async def webhook(input_endpoint: str, content: dict):
    send_to_airflow(input_endpoint, content)
    return {}
