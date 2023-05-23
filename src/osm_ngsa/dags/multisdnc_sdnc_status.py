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
import logging

from airflow import DAG
from airflow.decorators import task
from osm_mon.core.common_db import CommonDbClient
from osm_mon.core.config import Config
from osm_mon.sdnc_connectors.onos import OnosInfraCollector
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


SUPPORTED_SDNC_TYPES = ["onos_vpls"]
PROMETHEUS_PUSHGW = "pushgateway-prometheus-pushgateway:9091"
PROMETHEUS_JOB_PREFIX = "airflow_osm_sdnc_status_"
PROMETHEUS_METRIC = "osm_sdnc_status"
PROMETHEUS_METRIC_DESCRIPTION = "SDN Controller status"
SCHEDULE_INTERVAL = 1

# Logging
logger = logging.getLogger("airflow.task")


def get_all_sdnc():
    """Get SDNCs from MongoDB"""
    logger.info("Getting SDNC list")

    cfg = Config()
    logger.info(cfg.conf)
    common_db = CommonDbClient(cfg)
    sdnc_accounts = common_db.get_sdnc_accounts()
    sdnc_list = []
    for sdnc in sdnc_accounts:
        logger.info(f'Read SDNC {sdnc["_id"]} ({sdnc["name"]})')
        sdnc_list.append(
            {"_id": sdnc["_id"], "name": sdnc["name"], "type": sdnc["type"]}
        )

    logger.info(sdnc_list)
    logger.info("Getting SDNC list OK")
    return sdnc_list


def create_dag(dag_id, dag_number, dag_description, sdnc_id):
    dag = DAG(
        dag_id,
        catchup=False,
        default_args={
            "depends_on_past": False,
            "retries": 1,
            # "retry_delay": timedelta(minutes=1),
            "retry_delay": timedelta(seconds=10),
        },
        description=dag_description,
        is_paused_upon_creation=False,
        # schedule_interval=timedelta(minutes=SCHEDULE_INTERVAL),
        schedule_interval=f"*/{SCHEDULE_INTERVAL} * * * *",
        start_date=datetime(2022, 1, 1),
        tags=["osm", "sdnc"],
    )

    with dag:

        def get_sdnc_collector(sdnc_account):
            """Return a SDNC collector for the sdnc_account"""
            sdnc_type = sdnc_account["type"]
            if sdnc_type == "onos_vpls":
                return OnosInfraCollector(sdnc_account)
            logger.info(f"SDNC type '{sdnc_type}' not supported")
            return None

        @task(task_id="get_sdnc_status_and_send_to_prometheus")
        def get_sdnc_status_and_send_to_prometheus(sdnc_id: str):
            """Authenticate against SDN controller and check status"""

            # Get SDNC account info from MongoDB
            logger.info(f"Reading SDNC info, id: {sdnc_id}")
            cfg = Config()
            common_db = CommonDbClient(cfg)
            sdnc_account = common_db.get_sdnc_account(sdnc_account_id=sdnc_id)
            logger.info(sdnc_account)

            # Define Prometheus Metric for NS topology
            registry = CollectorRegistry()
            metric = Gauge(
                PROMETHEUS_METRIC,
                PROMETHEUS_METRIC_DESCRIPTION,
                labelnames=[
                    "sdnc_id",
                ],
                registry=registry,
            )
            metric.labels(sdnc_id).set(0)

            # Get status of SDNC
            collector = get_sdnc_collector(sdnc_account)
            if collector:
                status = collector.is_sdnc_ok()
                logger.info(f"SDNC status: {status}")
                metric.labels(sdnc_id).set(1)
            else:
                logger.info("Error creating SDNC collector")
            # Push to Prometheus
            push_to_gateway(
                gateway=PROMETHEUS_PUSHGW,
                job=f"{PROMETHEUS_JOB_PREFIX}{sdnc_id}",
                registry=registry,
            )
            return

        get_sdnc_status_and_send_to_prometheus(sdnc_id)

    return dag


sdnc_list = get_all_sdnc()
for index, sdnc in enumerate(sdnc_list):
    sdnc_type = sdnc["type"]
    if sdnc_type in SUPPORTED_SDNC_TYPES:
        sdnc_id = sdnc["_id"]
        sdnc_name = sdnc["name"]
        dag_description = f"Dag for SDNC {sdnc_name} status"
        dag_id = f"sdnc_status_{sdnc_id}"
        logger.info(f"Creating DAG {dag_id}")
        globals()[dag_id] = create_dag(
            dag_id=dag_id,
            dag_number=index,
            dag_description=dag_description,
            sdnc_id=sdnc_id,
        )
    else:
        logger.info(f"SDNC type '{sdnc_type}' not supported for monitoring SDNC status")
