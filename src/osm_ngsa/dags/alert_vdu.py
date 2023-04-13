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
import asyncio
from datetime import datetime, timedelta
import logging
import time
import uuid

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from osm_mon.core.common_db import CommonDbClient
from osm_mon.core.config import Config
from osm_mon.core.message_bus_client import MessageBusClient

# Logging
logger = logging.getLogger("airflow.task")


@dag(
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    description="Webhook callback for VDU alarm from Prometheus AlertManager",
    is_paused_upon_creation=False,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["osm", "webhook"],
)
def alert_vdu():
    @task(task_id="main_task")
    def main_task():
        logger.debug("Running main task...")
        context = get_current_context()
        conf = context["dag_run"].conf
        for alarm in conf["alerts"]:
            logger.info("VDU alarm:")
            status = alarm["status"]
            logger.info(f"  status: {status}")
            logger.info(f'  annotations: {alarm["annotations"]}')
            logger.info(f'  startsAt: {alarm["startsAt"]}')
            logger.info(f'  endsAt: {alarm["endsAt"]}')
            logger.info(f'  labels: {alarm["labels"]}')
            # vdu_down alert type
            if alarm["labels"]["alertname"] != "vdu_down":
                continue
            config = Config()
            common_db = CommonDbClient(config)
            ns_id = alarm["labels"]["ns_id"]
            vdu_name = alarm["labels"]["vdu_name"]
            vnf_member_index = alarm["labels"]["vnf_member_index"]
            vm_id = alarm["labels"]["vm_id"]
            if status == "firing":
                # Searching alerting rule in MongoDB
                logger.info(
                    f"Searching healing alert rule in MongoDB: ns_id {ns_id}, "
                    f"vnf_member_index {vnf_member_index}, "
                    f"vdu_name {vdu_name}, "
                    f"vm_id {vm_id}"
                )
                alert = common_db.get_alert(
                    nsr_id=ns_id,
                    vnf_member_index=vnf_member_index,
                    vdu_id=None,
                    vdu_name=vdu_name,
                    action_type="healing",
                )
                if alert:
                    logger.info("Found an alert rule:")
                    logger.info(alert)
                    # Update alert status
                    common_db.update_alert_status(
                        uuid=alert["uuid"], alarm_status="alarm"
                    )
                    # Get VNFR from MongoDB
                    vnfr = common_db.get_vnfr(
                        nsr_id=ns_id, member_index=vnf_member_index
                    )
                    logger.info(
                        f"Found VNFR ns_id: {ns_id}, vnf_member_index: {vnf_member_index}"
                    )
                    count_index = None
                    for vdu in vnfr.get("vdur", []):
                        if vdu["vim-id"] == vm_id:
                            count_index = vdu["count-index"]
                            break
                    if count_index is None:
                        logger.error(f"VDU {vm_id} not found in VNFR")
                        break
                    # Auto-healing type rule
                    vnf_id = alarm["labels"]["vnf_id"]
                    msg_bus = MessageBusClient(config)
                    loop = asyncio.get_event_loop()
                    _id = str(uuid.uuid4())
                    now = time.time()
                    vdu_id = alert["action"]["vdu-id"]
                    day1 = alert["action"]["day1"]
                    projects_read = vnfr["_admin"]["projects_read"]
                    projects_write = vnfr["_admin"]["projects_write"]
                    params = {
                        "lcmOperationType": "heal",
                        "nsInstanceId": ns_id,
                        "healVnfData": [
                            {
                                "vnfInstanceId": vnf_id,
                                "cause": "default",
                                "additionalParams": {
                                    "run-day1": day1,
                                    "vdu": [
                                        {
                                            "run-day1": day1,
                                            "count-index": count_index,
                                            "vdu-id": vdu_id,
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                    nslcmop = {
                        "id": _id,
                        "_id": _id,
                        "operationState": "PROCESSING",
                        "statusEnteredTime": now,
                        "nsInstanceId": ns_id,
                        "member-vnf-index": vnf_member_index,
                        "lcmOperationType": "heal",
                        "startTime": now,
                        "location": "default",
                        "isAutomaticInvocation": True,
                        "operationParams": params,
                        "isCancelPending": False,
                        "links": {
                            "self": "/osm/nslcm/v1/ns_lcm_op_occs/" + _id,
                            "nsInstance": "/osm/nslcm/v1/ns_instances/" + ns_id,
                        },
                        "_admin": {
                            "projects_read": projects_read,
                            "projects_write": projects_write,
                        },
                    }
                    common_db.create_nslcmop(nslcmop)
                    logger.info("Sending heal action message:")
                    logger.info(nslcmop)
                    loop.run_until_complete(msg_bus.aiowrite("ns", "heal", nslcmop))
                else:
                    logger.info("No alert rule was found")
            elif status == "resolved":
                # Searching alerting rule in MongoDB
                logger.info(
                    f"Searching alert rule in MongoDB: ns_id {ns_id}, "
                    f"vnf_member_index {vnf_member_index}, "
                    f"vdu_name {vdu_name}, "
                    f"vm_id {vm_id}"
                )
                alert = common_db.get_alert(
                    nsr_id=ns_id,
                    vnf_member_index=vnf_member_index,
                    vdu_id=None,
                    vdu_name=vdu_name,
                    action_type="healing",
                )
                if alert:
                    logger.info("Found an alert rule, updating status")
                    # Update alert status
                    common_db.update_alert_status(uuid=alert["uuid"], alarm_status="ok")

    main_task()


dag = alert_vdu()
