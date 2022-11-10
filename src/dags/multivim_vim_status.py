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
PROMETHEUS_JOB_PREFIX = "airflow_osm_vim_status_"
PROMETHEUS_METRIC = "vim_status"
PROMETHEUS_METRIC_DESCRIPTION = "VIM status"
SCHEDULE_INTERVAL = 1


def get_all_vim():
    """Get VIMs from MongoDB"""
    print("Getting VIM list")

    cfg = Config()
    print(cfg.conf)
    common_db = CommonDbClient(cfg)
    vim_accounts = common_db.get_vim_accounts()
    vim_list = []
    for vim in vim_accounts:
        print(f'Read VIM {vim["_id"]} ({vim["name"]})')
        vim_list.append(
            {"_id": vim["_id"], "name": vim["name"], "vim_type": vim["vim_type"]}
        )

    print(vim_list)
    print("Getting VIM list OK")
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
            if vim_type == "openstack":
                return OpenStackCollector(vim_account)
            if vim_type == "gcp":
                return GcpCollector(vim_account)
            if vim_type == "azure":
                return AzureCollector(vim_account)
            print(f"VIM type '{vim_type}' not supported")
            return None

        @task(task_id="get_vim_status_and_send_to_prometheus")
        def get_vim_status_and_send_to_prometheus(vim_id: str):
            """Authenticate against VIM and check status"""

            # Get VIM account info from MongoDB
            print(f"Reading VIM info, id: {vim_id}")
            cfg = Config()
            common_db = CommonDbClient(cfg)
            vim_account = common_db.get_vim_account(vim_account_id=vim_id)
            print(vim_account)

            # Define Prometheus Metric for NS topology
            registry = CollectorRegistry()
            metric = Gauge(
                PROMETHEUS_METRIC,
                PROMETHEUS_METRIC_DESCRIPTION,
                labelnames=[
                    "vim_id",
                ],
                registry=registry,
            )
            metric.labels(vim_id).set(0)

            # Get status of VIM
            collector = get_vim_collector(vim_account)
            if collector:
                status = collector.is_vim_ok()
                print(f"VIM status: {status}")
                metric.labels(vim_id).set(1)
            else:
                print("Error creating VIM collector")
            # Push to Prometheus
            push_to_gateway(
                gateway=PROMETHEUS_PUSHGW,
                job=f"{PROMETHEUS_JOB_PREFIX}{vim_id}",
                registry=registry,
            )
            return

        get_vim_status_and_send_to_prometheus(vim_id)

    return dag


vim_list = get_all_vim()
for index, vim in enumerate(vim_list):
    vim_type = vim["vim_type"]
    if vim_type in SUPPORTED_VIM_TYPES:
        vim_id = vim["_id"]
        vim_name = vim["name"]
        dag_description = f"Dag for VIM {vim_name} status"
        dag_id = f"vim_status_{vim_id}"
        print(f"Creating DAG {dag_id}")
        globals()[dag_id] = create_dag(
            dag_id=dag_id,
            dag_number=index,
            dag_description=dag_description,
            vim_id=vim_id,
        )
    else:
        print(f"VIM type '{vim_type}' not supported for monitoring VIM status")
