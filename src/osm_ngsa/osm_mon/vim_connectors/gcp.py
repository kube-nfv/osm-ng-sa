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
# pylint: disable=E1101

import logging
import time
from typing import Dict, List

from google.cloud import monitoring_v3
from google.oauth2 import service_account
import googleapiclient.discovery
from osm_mon.vim_connectors.base_vim import VIMConnector

log = logging.getLogger(__name__)


METRIC_MAPPINGS = {
    "cpu_utilization": {
        "metrictype": "compute.googleapis.com/instance/cpu/utilization",
        "multiplier": 100,
    },
    "average_memory_utilization": {
        # metric only available in e2 family
        "metrictype": "compute.googleapis.com/instance/memory/balloon/ram_used",
        "multiplier": 0.000001,
    },
    "disk_read_ops": {
        "metrictype": "compute.googleapis.com/instance/disk/read_ops_count",
    },
    "disk_write_ops": {
        "metrictype": "compute.googleapis.com/instance/disk/write_ops_count",
    },
    "disk_read_bytes": {
        "metrictype": "compute.googleapis.com/instance/disk/read_bytes_count",
    },
    "disk_write_bytes": {
        "metrictype": "compute.googleapis.com/instance/disk/write_bytes_count",
    },
    "packets_received": {
        "metrictype": "compute.googleapis.com/instance/network/received_packets_count",
    },
    "packets_sent": {
        "metrictype": "compute.googleapis.com/instance/network/sent_packets_count",
    },
    # "packets_in_dropped": {},
    # "packets_out_dropped": {},
}


class GcpCollector(VIMConnector):
    def __init__(self, vim_account: Dict):
        self.vim_account = vim_account
        self.project = vim_account["vim_tenant_name"] or vim_account["vim_tenant_id"]

        # REGION - Google Cloud considers regions and zones. A specific region
        # can have more than one zone (for instance: region us-west1 with the
        # zones us-west1-a, us-west1-b and us-west1-c). So the region name
        # specified in the config will be considered as a specific zone for GC
        # and the region will be calculated from that without the preffix.
        if "config" in vim_account:
            config = vim_account["config"]
            if "region_name" in config:
                self.zone = config.get("region_name")
                self.region = self.zone.rsplit("-", 1)[0]
            else:
                log.error("Google Cloud region_name not specified in config")
        else:
            log.error("config is not specified in VIM")

        # Credentials
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.credentials = None
        if "credentials" in config:
            log.debug("Setting credentials")
            # Settings Google Cloud credentials dict
            creds_body = config["credentials"]
            creds = service_account.Credentials.from_service_account_info(creds_body)
            if "sa_file" in config:
                creds = service_account.Credentials.from_service_account_file(
                    config.get("sa_file"), scopes=scopes
                )
                log.debug("Credentials: %s", creds)
            # Construct a Resource for interacting with an API.
            self.credentials = creds
            try:
                self.conn_compute = googleapiclient.discovery.build(
                    "compute", "v1", credentials=creds
                )
            except Exception as e:
                log.error(e)
            # Construct a client for interacting with metrics API.
            try:
                self.metric_client = monitoring_v3.MetricServiceClient(
                    credentials=creds
                )
            except Exception as e:
                log.error(e)
        else:
            log.error("It is not possible to init GCP with no credentials")

    def collect_servers_status(self) -> List[Dict]:
        servers = []
        try:
            response = (
                self.conn_compute.instances()
                .list(project=self.project, zone=self.zone)
                .execute()
            )
            if "items" in response:
                log.info(response["items"])
                for server in response["items"]:
                    vm = {
                        "id": server["name"],
                        "name": server["name"],
                        "status": (1 if (server["status"] == "RUNNING") else 0),
                    }
                    servers.append(vm)
        except Exception as e:
            log.error(e)
        return servers

    def is_vim_ok(self) -> bool:
        status = False
        try:
            self.conn_compute.zones().get(
                project=self.project, zone=self.zone
            ).execute()
            status = True
        except Exception as e:
            log.error(e)
        return status

    def collect_metrics(self, metric_list: List[Dict]) -> List[Dict]:
        log.debug("collect_metrics")

        metric_results = []
        log.info(metric_list)
        for metric in metric_list:
            server = metric["vm_id"]
            metric_name = metric["metric"]
            metric_mapping = METRIC_MAPPINGS.get(metric_name)
            if not metric_mapping:
                # log.info(f"Metric {metric_name} not available in GCP")
                continue
            gcp_metric_type = metric_mapping["metrictype"]
            metric_multiplier = metric_mapping.get("multiplier", 1)
            log.info(f"server: {server}, gcp_metric_type: {gcp_metric_type}")

            end = int(time.time())
            start = end - 600
            interval = monitoring_v3.TimeInterval(
                {
                    "end_time": {"seconds": end},
                    "start_time": {"seconds": start},
                }
            )
            aggregation = monitoring_v3.Aggregation(
                {
                    "alignment_period": {"seconds": 600},
                    "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
                }
            )
            filter = f'metric.type = "{gcp_metric_type}" AND metric.labels.instance_name = "{server}"'
            project = f"projects/{self.project}"
            log.info(f"filter: {filter}")
            results = self.metric_client.list_time_series(
                request={
                    "name": project,
                    "filter": filter,
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    "aggregation": aggregation,
                }
            )
            value = None
            for result in results:
                for point in result.points:
                    value = point.value.double_value
            if value is not None:
                metric["value"] = value * metric_multiplier
                log.info(f'value: {metric["value"]}')
                metric_results.append(metric)

        return metric_results
