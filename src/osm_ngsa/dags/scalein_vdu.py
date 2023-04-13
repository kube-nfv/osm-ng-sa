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
        "retry_delay": timedelta(seconds=15),
    },
    description="Webhook callback for scale-in alarm from Prometheus AlertManager",
    is_paused_upon_creation=False,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["osm", "webhook"],
)
def scalein_vdu():
    @task(task_id="main_task")
    def main_task():
        logger.debug("Running main task...")
        # Read input parameters
        context = get_current_context()
        conf = context["dag_run"].conf
        for alarm in conf["alerts"]:
            logger.info("Scale-in alarm:")
            status = alarm["status"]
            logger.info(f"  status: {status}")
            logger.info(f'  annotations: {alarm["annotations"]}')
            logger.info(f'  startsAt: {alarm["startsAt"]}')
            logger.info(f'  endsAt: {alarm["endsAt"]}')
            logger.info(f'  labels: {alarm["labels"]}')
            alertname = alarm["labels"].get("alertname")
            if not alertname.startswith("scalein_"):
                continue
            # scalein_vdu alert type
            config = Config()
            common_db = CommonDbClient(config)
            ns_id = alarm["labels"]["ns_id"]
            vdu_id = alarm["labels"]["vdu_id"]
            vnf_member_index = alarm["labels"]["vnf_member_index"]
            if status == "firing":
                # Searching alerting rule in MongoDB
                logger.info(
                    f"Searching scale-in alert rule in MongoDB: ns_id {ns_id}, "
                    f"vnf_member_index {vnf_member_index}, "
                    f"vdu_id {vdu_id}, "
                )
                alert = common_db.get_alert(
                    nsr_id=ns_id,
                    vnf_member_index=vnf_member_index,
                    vdu_id=vdu_id,
                    vdu_name=None,
                    action_type="scale_in",
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
                    # Check cooldown-time before scale-in
                    send_lcm = 1
                    if "cooldown-time" in alert["action"]:
                        cooldown_time = alert["action"]["cooldown-time"]
                        cooldown_time = cooldown_time * 60
                        now = time.time()
                        since = now - cooldown_time
                        logger.info(
                            f"Looking for scale operations in cooldown interval ({cooldown_time} s)"
                        )
                        nslcmops = common_db.get_nslcmop(
                            nsr_id=ns_id, operation_type="scale", since=since
                        )
                        op = next(
                            (
                                sub
                                for sub in nslcmops
                                if ("scaleVnfData" in sub["operationParams"])
                                and (
                                    "scaleByStepData"
                                    in sub["operationParams"]["scaleVnfData"]
                                )
                                and (
                                    "member-vnf-index"
                                    in sub["operationParams"]["scaleVnfData"][
                                        "scaleByStepData"
                                    ]
                                )
                                and (
                                    sub["operationParams"]["scaleVnfData"][
                                        "scaleByStepData"
                                    ]["member-vnf-index"]
                                    == vnf_member_index
                                )
                            ),
                            None,
                        )
                        if op:
                            logger.info(
                                f"No scale-in will be launched, found a previous scale operation in cooldown interval: {op}"
                            )
                            send_lcm = 0

                    if send_lcm:
                        # Save nslcmop object in MongoDB
                        msg_bus = MessageBusClient(config)
                        loop = asyncio.get_event_loop()
                        _id = str(uuid.uuid4())
                        now = time.time()
                        projects_read = vnfr["_admin"]["projects_read"]
                        projects_write = vnfr["_admin"]["projects_write"]
                        scaling_group = alert["action"]["scaling-group"]
                        params = {
                            "scaleType": "SCALE_VNF",
                            "scaleVnfData": {
                                "scaleVnfType": "SCALE_IN",
                                "scaleByStepData": {
                                    "scaling-group-descriptor": scaling_group,
                                    "member-vnf-index": vnf_member_index,
                                },
                            },
                            "scaleTime": "{}Z".format(datetime.utcnow().isoformat()),
                        }
                        nslcmop = {
                            "id": _id,
                            "_id": _id,
                            "operationState": "PROCESSING",
                            "statusEnteredTime": now,
                            "nsInstanceId": ns_id,
                            "lcmOperationType": "scale",
                            "startTime": now,
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
                        # Send Kafka message to LCM
                        logger.info("Sending scale-in action message:")
                        logger.info(nslcmop)
                        loop.run_until_complete(
                            msg_bus.aiowrite("ns", "scale", nslcmop)
                        )
                else:
                    logger.info("No alert rule was found")
            elif status == "resolved":
                # Searching alerting rule in MongoDB
                logger.info(
                    f"Searching alert rule in MongoDB: ns_id {ns_id}, "
                    f"vnf_member_index {vnf_member_index}, "
                )
                alert = common_db.get_alert(
                    nsr_id=ns_id,
                    vnf_member_index=vnf_member_index,
                    vdu_id=vdu_id,
                    vdu_name=None,
                    action_type="scale_in",
                )
                if alert:
                    logger.info("Found an alert rule, updating status")
                    # Update alert status
                    common_db.update_alert_status(uuid=alert["uuid"], alarm_status="ok")

    main_task()


dag = scalein_vdu()
