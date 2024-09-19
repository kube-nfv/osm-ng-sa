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

from airflow.decorators import dag, task
from osm_ngsa.osm_mon.core.common_db import CommonDbClient
from osm_ngsa.osm_mon.core.config import Config
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


PROMETHEUS_PUSHGW = "pushgateway-prometheus-pushgateway:9091"
PROMETHEUS_JOB = "airflow_osm_ns_topology"
PROMETHEUS_METRIC = "ns_topology"
PROMETHEUS_METRIC_DESCRIPTION = "Network services topology"
SCHEDULE_INTERVAL = 2

# Logging
logger = logging.getLogger("airflow.task")


@dag(
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description="NS topology",
    is_paused_upon_creation=False,
    max_active_runs=1,
    # schedule_interval=timedelta(minutes=SCHEDULE_INTERVAL),
    schedule_interval=f"*/{SCHEDULE_INTERVAL} * * * *",
    start_date=datetime(2022, 1, 1),
    tags=["osm", "topology"],
)
def ns_topology():
    @task(task_id="get_topology")
    def get_topology():
        """
        Get NS topology from MongoDB and exports it as a metric
        to Prometheus
        """

        # Define Prometheus Metric for NS topology
        registry = CollectorRegistry()
        metric = Gauge(
            PROMETHEUS_METRIC,
            PROMETHEUS_METRIC_DESCRIPTION,
            labelnames=[
                "ns_id",
                "project_id",
                "vnf_id",
                "vdu_id",
                "vm_id",
                "vim_id",
                "vdu_name",
                "vnf_member_index",
                "ns_name",
            ],
            registry=registry,
        )

        # Getting VNFR list from MongoDB
        logger.info("Getting VNFR list from MongoDB")
        cfg = Config()
        # logger.debug(cfg.conf)
        common_db = CommonDbClient(cfg)
        vnfr_list = common_db.get_vnfrs()

        # Only send topology if ns state is one of the nsAllowedStatesSet
        nsAllowedStatesSet = {"INSTANTIATED"}

        # For loop to get NS topology.
        # For each VDU, a metric sample is created with the appropriate labels
        for vnfr in vnfr_list:
            # Label ns_id
            vnf_id = vnfr["_id"]
            # Label ns_id
            ns_id = vnfr["nsr-id-ref"]
            nsr = common_db.get_nsr(ns_id)
            ns_name = nsr["name"]
            # Label vnfd_id
            vnfd_id = vnfr["vnfd-ref"]
            # Label project_id
            project_list = vnfr.get("_admin", {}).get("projects_read", [])
            project_id = "None"
            if project_list:
                project_id = project_list[0]
            # Other info
            ns_state = vnfr["_admin"]["nsState"]
            vnf_membex_index = vnfr["member-vnf-index-ref"]
            logger.info(
                f"Read VNFR: id: {vnf_id}, ns_id: {ns_id}, ns_name: {ns_name} "
                f"state: {ns_state}, vnfd_id: {vnfd_id}, "
                f"vnf_membex_index: {vnf_membex_index}, "
                f"project_id: {project_id}"
            )
            # Only send topology if ns State is one of the nsAllowedStatesSet
            if ns_state not in nsAllowedStatesSet:
                continue

            logger.debug("VDU list:")
            for vdu in vnfr.get("vdur", []):
                # Label vdu_id
                vdu_id = vdu["_id"]
                # Label vim_id
                vim_info = vdu.get("vim_info")
                if not vim_info:
                    logger.info("Error: vim_info not available in vdur")
                    continue
                if len(vim_info) != 1:
                    logger.info("Error: more than one vim_info in vdur")
                    continue
                vim_id = next(iter(vim_info))[4:]
                # TODO: check if it makes sense to use vnfr.vim-account-id as vim_id instead of the vim_info key
                # Label vm_id
                vm_id = vdu.get("vim-id")
                if not vm_id:
                    logger.info("Error: vim-id not available in vdur")
                    continue
                # Other VDU info
                vdu_name = vdu.get("name", "UNKNOWN")
                logger.debug(
                    f"  VDU id: {vdu_id}, name: {vdu_name}, "
                    f"vim_id: {vim_id}, vm_id: {vm_id}"
                )
                logger.info(
                    f"METRIC SAMPLE: ns_id: {ns_id}, ns_name: {ns_name}"
                    f"project_id: {project_id}, vnf_id: {vnf_id}, "
                    f"vdu_id: {vdu_id}, vm_id: {vm_id}, vim_id: {vim_id}"
                )
                metric.labels(
                    ns_id,
                    project_id,
                    vnf_id,
                    vdu_id,
                    vm_id,
                    vim_id,
                    vdu_name,
                    vnf_membex_index,
                    ns_name,
                ).set(1)

        push_to_gateway(
            gateway=PROMETHEUS_PUSHGW, job=PROMETHEUS_JOB, registry=registry
        )
        return

    get_topology()


dag = ns_topology()
