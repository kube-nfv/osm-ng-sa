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
from osm_mon.vim_connectors.azure import AzureCollector
from osm_mon.vim_connectors.gcp import GcpCollector
from osm_mon.vim_connectors.openstack import OpenStackCollector
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


SUPPORTED_VIM_TYPES = ["openstack", "vio", "gcp", "azure"]
PROMETHEUS_PUSHGW = "pushgateway-prometheus-pushgateway:9091"
PROMETHEUS_JOB_PREFIX = "airflow_osm_vm_status_"
PROMETHEUS_METRIC = "vm_status"
PROMETHEUS_METRIC_DESCRIPTION = "VM Status from VIM"
SCHEDULE_INTERVAL = 1

# Logging
logger = logging.getLogger("airflow.task")


def get_all_vim():
    """Get VIMs from MongoDB"""
    logger.info("Getting VIM list")

    cfg = Config()
    logger.info(cfg.conf)
    common_db = CommonDbClient(cfg)
    vim_accounts = common_db.get_vim_accounts()
    vim_list = []
    for vim in vim_accounts:
        logger.info(f'Read VIM {vim["_id"]} ({vim["name"]})')
        vim_list.append(
            {"_id": vim["_id"], "name": vim["name"], "vim_type": vim["vim_type"]}
        )

    logger.info(vim_list)
    logger.info("Getting VIM list OK")
    return vim_list


def create_dag(dag_id, dag_number, dag_description, vim_id):
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
        max_active_runs=1,
        # schedule_interval=timedelta(minutes=SCHEDULE_INTERVAL),
        schedule_interval=f"*/{SCHEDULE_INTERVAL} * * * *",
        start_date=datetime(2022, 1, 1),
        tags=["osm", "vim"],
    )

    with dag:

        def get_vim_collector(vim_account):
            """Return a VIM collector for the vim_account"""
            vim_type = vim_account["vim_type"]
            if "config" in vim_account and "vim_type" in vim_account["config"]:
                vim_type = vim_account["config"]["vim_type"].lower()
                if vim_type == "vio" and "vrops_site" not in vim_account["config"]:
                    vim_type = "openstack"
            if vim_type == "openstack" or vim_type == "vio":
                return OpenStackCollector(vim_account)
            if vim_type == "gcp":
                return GcpCollector(vim_account)
            if vim_type == "azure":
                return AzureCollector(vim_account)
            logger.info(f"VIM type '{vim_type}' not supported")
            return None

        def get_all_vm_status(vim_account):
            """Get VM status from the VIM"""
            collector = get_vim_collector(vim_account)
            if collector:
                status = collector.is_vim_ok()
                logger.info(f"VIM status: {status}")
                vm_status_list = collector.collect_servers_status()
                return vm_status_list
            else:
                logger.error("No collector for VIM")
                return []

        @task(task_id="get_all_vm_status_and_send_to_prometheus")
        def get_all_vm_status_and_send_to_prometheus(vim_id: str):
            """Authenticate against VIM, collect servers status and send to prometheus"""

            # Get VIM account info from MongoDB
            logger.info(f"Reading VIM info, id: {vim_id}")
            cfg = Config()
            common_db = CommonDbClient(cfg)
            vim_account = common_db.get_vim_account(vim_account_id=vim_id)
            logger.info(vim_account)

            # Define Prometheus Metric for NS topology
            registry = CollectorRegistry()
            metric = Gauge(
                PROMETHEUS_METRIC,
                PROMETHEUS_METRIC_DESCRIPTION,
                labelnames=[
                    "vm_id",
                    "vim_id",
                ],
                registry=registry,
            )

            # Get status of all VM from VIM
            all_vm_status = get_all_vm_status(vim_account)
            logger.info(f"Got {len(all_vm_status)} VMs with their status:")
            for vm in all_vm_status:
                vm_id = vm["id"]
                vm_status = vm["status"]
                vm_name = vm.get("name", "")
                logger.info(f"    {vm_name} ({vm_id}) {vm_status}")
                metric.labels(vm_id, vim_id).set(vm_status)
            # Push to Prometheus
            push_to_gateway(
                gateway=PROMETHEUS_PUSHGW,
                job=f"{PROMETHEUS_JOB_PREFIX}{vim_id}",
                registry=registry,
            )
            return

        get_all_vm_status_and_send_to_prometheus(vim_id)

    return dag


vim_list = get_all_vim()
for index, vim in enumerate(vim_list):
    vim_type = vim["vim_type"]
    if vim_type in SUPPORTED_VIM_TYPES:
        vim_id = vim["_id"]
        vim_name = vim["name"]
        dag_description = f"Dag for vim {vim_name}"
        dag_id = f"vm_status_vim_{vim_id}"
        logger.info(f"Creating DAG {dag_id}")
        globals()[dag_id] = create_dag(
            dag_id=dag_id,
            dag_number=index,
            dag_description=dag_description,
            vim_id=vim_id,
        )
    else:
        logger.info(f"VIM type '{vim_type}' not supported for collecting VM status")
