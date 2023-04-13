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
from math import ceil
from typing import Dict, List

from airflow import DAG
from airflow.decorators import task
from osm_mon.core.common_db import CommonDbClient
from osm_mon.core.config import Config
from osm_mon.vim_connectors.openstack import OpenStackCollector
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


SCHEDULE_INTERVAL = 5
COLLECTOR_MAX_METRICS_PER_TASK = 100
SUPPORTED_VIM_TYPES = ["openstack", "vio"]
PROMETHEUS_PUSHGW = "pushgateway-prometheus-pushgateway:9091"
PROMETHEUS_JOB_PREFIX = "airflow_osm_vm_metrics_"
PROMETHEUS_METRICS = {
    "cpu_utilization": {
        "metric_name": "cpu_utilization",
        "metric_descr": "CPU usage percentage",
    },
    "average_memory_utilization": {
        "metric_name": "average_memory_utilization",
        "metric_descr": "Volume of RAM in MB used by the VM",
    },
    "disk_read_ops": {
        "metric_name": "disk_read_ops",
        "metric_descr": "Number of read requests",
    },
    "disk_write_ops": {
        "metric_name": "disk_write_ops",
        "metric_descr": "Number of write requests",
    },
    "disk_read_bytes": {
        "metric_name": "disk_read_bytes",
        "metric_descr": "Volume of reads in bytes",
    },
    "disk_write_bytes": {
        "metric_name": "disk_write_bytes",
        "metric_descr": "Volume of writes in bytes",
    },
    "packets_received": {
        "metric_name": "packets_received",
        "metric_descr": "Number of incoming packets",
    },
    "packets_sent": {
        "metric_name": "packets_sent",
        "metric_descr": "Number of outgoing packets",
    },
    "packets_in_dropped": {
        "metric_name": "packets_in_dropped",
        "metric_descr": "Number of incoming dropped packets",
    },
    "packets_out_dropped": {
        "metric_name": "packets_out_dropped",
        "metric_descr": "Number of outgoing dropped packets",
    },
}

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
        # schedule_interval=timedelta(minutes=SCHEDULE_INTERVAL),
        schedule_interval=f"*/{SCHEDULE_INTERVAL} * * * *",
        start_date=datetime(2022, 1, 1),
        tags=["osm", "vdu"],
    )

    with dag:

        @task(task_id="extract_metrics_from_vnfrs")
        def extract_metrics_from_vnfrs(vim_id: str):
            """Get metric list to collect from VIM based on VNFDs and VNFRs stored in MongoDB"""

            # Get VNFDs that include "monitoring-parameter" from MongoDB
            cfg = Config()
            common_db = CommonDbClient(cfg)
            logger.info("Getting VNFDs with monitoring parameters from MongoDB")
            vnfd_list = common_db.get_monitoring_vnfds()
            # Get VNFR list from MongoDB
            logger.info("Getting VNFR list from MongoDB")
            vnfr_list = common_db.get_vnfrs(vim_account_id=vim_id)
            # Only read metrics if ns state is one of the nsAllowedStatesSet
            nsAllowedStatesSet = {"INSTANTIATED"}
            metric_list = []
            for vnfr in vnfr_list:
                if vnfr["_admin"]["nsState"] not in nsAllowedStatesSet:
                    continue
                # Check if VNFR is in "monitoring-parameter" VNFDs list
                vnfd_id = vnfr["vnfd-id"]
                vnfd = next(
                    (item for item in vnfd_list if item["_id"] == vnfd_id), None
                )
                if not vnfd:
                    continue
                ns_id = vnfr["nsr-id-ref"]
                vnf_index = vnfr["member-vnf-index-ref"]
                logger.info(
                    f"NS {ns_id}, vnf-index {vnf_index} has monitoring-parameter"
                )
                project_list = vnfr.get("_admin", {}).get("projects_read", [])
                project_id = "None"
                if project_list:
                    project_id = project_list[0]
                for vdur in vnfr.get("vdur", []):
                    vim_info = vdur.get("vim_info")
                    if not vim_info:
                        logger.error("Error: vim_info not available in vdur")
                        continue
                    if len(vim_info) != 1:
                        logger.error("Error: more than one vim_info in vdur")
                        continue
                    vim_id = next(iter(vim_info))[4:]
                    vm_id = vdur.get("vim-id")
                    if not vm_id:
                        logger.error("Error: vim-id not available in vdur")
                        continue
                    vdu_name = vdur.get("name", "UNKNOWN")
                    vdu = next(
                        filter(lambda vdu: vdu["id"] == vdur["vdu-id-ref"], vnfd["vdu"])
                    )
                    if "monitoring-parameter" not in vdu:
                        logger.error("Error: no monitoring-parameter in descriptor")
                        continue
                    for param in vdu["monitoring-parameter"]:
                        metric_name = param["performance-metric"]
                        metric_id = param["id"]
                        metric = {
                            "metric": metric_name,
                            "metric_id": metric_id,
                            "vm_id": vm_id,
                            "ns_id": ns_id,
                            "project_id": project_id,
                            "vdu_name": vdu_name,
                            "vnf_member_index": vnf_index,
                            "vdu_id": vdu["id"],
                        }
                        metric_list.append(metric)

            logger.info(f"Metrics to collect: {len(metric_list)}")
            return metric_list

        @task(task_id="split_metrics")
        def split_metrics(metric_list: List[Dict]):
            """Split metric list based on COLLECTOR_MAX_METRICS_PER_TASK"""
            n_metrics = len(metric_list)
            if n_metrics <= COLLECTOR_MAX_METRICS_PER_TASK:
                return [metric_list]
            metrics_per_chunk = ceil(
                n_metrics / ceil(n_metrics / COLLECTOR_MAX_METRICS_PER_TASK)
            )
            logger.info(
                f"Splitting {n_metrics} metrics into chunks of {metrics_per_chunk} items"
            )
            chunks = []
            for i in range(0, n_metrics, metrics_per_chunk):
                chunks.append(metric_list[i : i + metrics_per_chunk])
            return chunks

        @task(task_id="collect_metrics")
        def collect_metrics(vim_id: str, metric_list: List[Dict]):
            """Collect servers metrics"""
            if not metric_list:
                return []

            # Get VIM account info from MongoDB
            logger.info(f"Reading VIM info, id: {vim_id}")
            cfg = Config()
            common_db = CommonDbClient(cfg)
            vim_account = common_db.get_vim_account(vim_account_id=vim_id)
            # Create VIM metrics collector
            vim_type = vim_account["vim_type"]
            if "config" in vim_account and "vim_type" in vim_account["config"]:
                vim_type = vim_account["config"]["vim_type"].lower()
                if vim_type == "vio" and "vrops_site" not in vim_account["config"]:
                    vim_type = "openstack"
            if vim_type == "openstack":
                collector = OpenStackCollector(vim_account)
            else:
                logger.error(f"VIM type '{vim_type}' not supported")
                return None
            # Get metrics
            results = []
            if collector:
                results = collector.collect_metrics(metric_list)
            logger.info(results)
            return results

        @task(task_id="send_prometheus")
        def send_prometheus(metric_lists: List[List[Dict]]):
            """Send servers metrics to Prometheus Push Gateway"""
            logger.info(metric_lists)

            # Define Prometheus metrics
            registry = CollectorRegistry()
            prom_metrics = {}
            prom_metrics_keys = PROMETHEUS_METRICS.keys()
            for key in prom_metrics_keys:
                prom_metrics[key] = Gauge(
                    PROMETHEUS_METRICS[key]["metric_name"],
                    PROMETHEUS_METRICS[key]["metric_descr"],
                    labelnames=[
                        "metric_id",
                        "ns_id",
                        "project_id",
                        "vnf_member_index",
                        "vm_id",
                        "vim_id",
                        "vdu_name",
                        "vdu_id",
                    ],
                    registry=registry,
                )

            for metric_list in metric_lists:
                for metric in metric_list:
                    metric_name = metric["metric"]
                    metric_id = metric["metric_id"]
                    value = metric["value"]
                    vm_id = metric["vm_id"]
                    vm_name = metric.get("vdu_name", "")
                    ns_id = metric["ns_id"]
                    project_id = metric["project_id"]
                    vnf_index = metric["vnf_member_index"]
                    vdu_id = metric["vdu_id"]
                    logger.info(
                        f"  metric: {metric_name}, metric_id: {metric_id} value: {value}, vm_id: {vm_id}, vm_name: {vm_name}, ns_id: {ns_id}, vnf_index: {vnf_index}, project_id: {project_id}, vdu_id: {vdu_id} "
                    )
                    if metric_name in prom_metrics_keys:
                        prom_metrics[metric_name].labels(
                            metric_id,
                            ns_id,
                            project_id,
                            vnf_index,
                            vm_id,
                            vim_id,
                            vm_name,
                            vdu_id,
                        ).set(value)

            # Push to Prometheus
            push_to_gateway(
                gateway=PROMETHEUS_PUSHGW,
                job=f"{PROMETHEUS_JOB_PREFIX}{vim_id}",
                registry=registry,
            )
            return

        chunks = split_metrics(extract_metrics_from_vnfrs(vim_id))
        send_prometheus(
            collect_metrics.partial(vim_id=vim_id).expand(metric_list=chunks)
        )

    return dag


vim_list = get_all_vim()
for index, vim in enumerate(vim_list):
    vim_type = vim["vim_type"]
    if vim_type in SUPPORTED_VIM_TYPES:
        vim_id = vim["_id"]
        vim_name = vim["name"]
        dag_description = f"Dag for collecting VM metrics from VIM {vim_name}"
        dag_id = f"vm_metrics_vim_{vim_id}"
        logger.info(f"Creating DAG {dag_id}")
        globals()[dag_id] = create_dag(
            dag_id=dag_id,
            dag_number=index,
            dag_description=dag_description,
            vim_id=vim_id,
        )
    else:
        logger.info(f"VIM type '{vim_type}' not supported for collecting VM metrics")
