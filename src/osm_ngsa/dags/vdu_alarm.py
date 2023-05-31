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
from datetime import datetime, timedelta
import json
import logging

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from osm_mon.core.common_db import CommonDbClient
from osm_mon.core.config import Config
import requests
from requests.exceptions import ConnectionError, RequestException

# Logging
logger = logging.getLogger("airflow.task")


@dag(
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=15),
    },
    description="Webhook callback for VDU alarm from Prometheus AlertManager",
    is_paused_upon_creation=False,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["osm", "webhook"],
)
def vdu_alarm():
    @task(task_id="main_task")
    def main_task():
        logger.debug("Running main task...")
        # Read input parameters
        context = get_current_context()
        conf = context["dag_run"].conf
        for alarm in conf["alerts"]:
            logger.info("Alarm:")
            status = alarm["status"]
            logger.info(f"  status: {status}")
            logger.info(f'  annotations: {alarm["annotations"]}')
            logger.info(f'  startsAt: {alarm["startsAt"]}')
            logger.info(f'  endsAt: {alarm["endsAt"]}')
            logger.info(f'  labels: {alarm["labels"]}')
            alertname = alarm["labels"].get("alertname")
            # Check vdu_alarm alert type
            if not alertname.startswith("vdu_alarm_"):
                continue
            config = Config()
            common_db = CommonDbClient(config)
            ns_id = alarm["labels"]["ns_id"]
            vdu_id = alarm["labels"]["vdu_id"]
            vnf_member_index = alarm["labels"]["vnf_member_index"]
            # Searching alerting rule in MongoDB
            logger.info(
                f"Searching alarm rule in MongoDB: ns_id {ns_id}, "
                f"vnf_member_index {vnf_member_index}, "
                f"vdu_id {vdu_id}, "
            )
            alert = common_db.get_alert(
                nsr_id=ns_id,
                vnf_member_index=vnf_member_index,
                vdu_id=vdu_id,
                vdu_name=None,
                action_type="vdu_alarm",
            )
            if alert:
                logger.info("Found an alert rule:")
                logger.info(alert)
                if status == "firing":
                    alarm_status = "alarm"
                elif status == "resolved":
                    alarm_status = "ok"
                else:
                    continue
                # Update alert status
                common_db.update_alert_status(
                    uuid=alert["uuid"], alarm_status=alarm_status
                )
                if alarm_status in alert["action"]:
                    urls = []
                    for item in alert["action"][alarm_status]:
                        urls.append(item["url"])
                else:
                    logger.info(f"No '{alarm_status}' action in the alert rule")
                    continue
                # Send HTTP request
                notify_details = {
                    "alarm_uuid": alert["uuid"],
                    "status": alarm_status,
                    "metric_name": alert["metric"],
                    "start_date": alarm["startsAt"],
                    "tags": alarm["labels"],
                }
                payload = {
                    "schema_type": "notify_alarm",
                    "schema_version": "1.1",
                    "notify_details": notify_details,
                }
                headers = {"content-type": "application/json"}
                for url in urls:
                    logger.info(f"Sending HTTP POST to {url}...")
                    try:
                        resp = requests.post(
                            url=url,
                            data=json.dumps(payload),
                            headers=headers,
                            verify=False,
                            timeout=15,
                        )
                        logger.info(f"Response {resp}")
                    except ConnectionError:
                        logger.info(f"Error connecting to url {url}")
                    except RequestException as e:
                        logger.info(
                            f"RequestException while connecting to url {url}: {e}"
                        )

    main_task()


dag = vdu_alarm()
