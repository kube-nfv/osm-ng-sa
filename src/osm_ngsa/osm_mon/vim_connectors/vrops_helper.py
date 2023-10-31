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

import json
import logging
import traceback

import requests

log = logging.getLogger(__name__)

# Ref: https://docs.vmware.com/en/vRealize-Operations-Manager/7.0/vrealize-operations-manager-70-reference-guide.pdf
# Potential metrics of interest
# "cpu|capacity_contentionPct"
# "cpu|corecount_provisioned"
# "cpu|costopPct"
# "cpu|demandmhz"
# "cpu|demandPct"
# "cpu|effective_limit"
# "cpu|iowaitPct"
# "cpu|readyPct"
# "cpu|swapwaitPct"
# "cpu|usage_average"
# "cpu|usagemhz_average"
# "cpu|usagemhz_average_mtd"
# "cpu|vm_capacity_provisioned"
# "cpu|workload"
# "guestfilesystem|percentage_total"
# "guestfilesystem|usage_total"
# "mem|consumedPct"
# "mem|guest_usage"
# "mem|host_contentionPct"
# "mem|reservation_used"
# "mem|swapinRate_average"
# "mem|swapoutRate_average"
# "mem|swapped_average"
# "mem|usage_average"
# "net:Aggregate of all instances|droppedPct"
# "net|broadcastTx_summation"
# "net|droppedTx_summation"
# "net|multicastTx_summation"
# "net|pnicBytesRx_average"
# "net|pnicBytesTx_average"
# "net|received_average"
# "net|transmitted_average"
# "net|usage_average"
# "virtualDisk:Aggregate of all instances|commandsAveraged_average"
# "virtualDisk:Aggregate of all instances|numberReadAveraged_average"
# "virtualDisk:Aggregate of all instances|numberWriteAveraged_average"
# "virtualDisk:Aggregate of all instances|totalLatency"
# "virtualDisk:Aggregate of all instances|totalReadLatency_average"
# "virtualDisk:Aggregate of all instances|totalWriteLatency_average"
# "virtualDisk:Aggregate of all instances|usage"
# "virtualDisk:Aggregate of all instances|vDiskOIO"
# "virtualDisk|read_average"
# "virtualDisk|write_average"
METRIC_MAPPINGS = {
    # Percent guest operating system active memory.
    "average_memory_utilization": "mem|usage_average",
    # Percentage of CPU that was used out of all the CPU that was allocated.
    "cpu_utilization": "cpu|usage_average",
    # KB/s of data read in the performance interval
    "disk_read_bytes": "virtualDisk|read_average",
    # Average of read commands per second during the collection interval.
    "disk_read_ops": "virtualDisk:aggregate of all instances|numberReadAveraged_average",
    # KB/s of data written in the performance interval.
    "disk_write_bytes": "virtualDisk|write_average",
    # Average of write commands per second during the collection interval.
    "disk_write_ops": "virtualDisk:aggregate of all instances|numberWriteAveraged_average",
    # Not supported by vROPS, will always return 0.
    "packets_in_dropped": "net|droppedRx_summation",
    # Transmitted packets dropped in the collection interval.
    "packets_out_dropped": "net|droppedTx_summation",
    # Bytes received in the performance interval.
    "packets_received": "net|received_average",
    # Packets transmitted in the performance interval.
    "packets_sent": "net|transmitted_average",
}

# If the unit from vROPS does not align with the expected value. multiply by the specified amount to ensure
# the correct unit is returned.
METRIC_MULTIPLIERS = {
    "disk_read_bytes": 1024,
    "disk_write_bytes": 1024,
    "packets_received": 1024,
    "packets_sent": 1024,
}


class vROPS_Helper:
    def __init__(self, vrops_site="https://vrops", vrops_user="", vrops_password=""):
        self.vrops_site = vrops_site
        self.vrops_user = vrops_user
        self.vrops_password = vrops_password

    def get_vrops_token(self):
        """Fetches token from vrops"""
        auth_url = "/suite-api/api/auth/token/acquire"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        req_body = {"username": self.vrops_user, "password": self.vrops_password}
        resp = requests.post(
            self.vrops_site + auth_url, json=req_body, verify=False, headers=headers
        )
        if resp.status_code != 200:
            log.error(
                "Failed to get token from vROPS: {} {}".format(
                    resp.status_code, resp.content
                )
            )
            return None

        resp_data = json.loads(resp.content.decode("utf-8"))
        return resp_data["token"]

    def get_vm_resource_list_from_vrops(self):
        """Find all known resource IDs in vROPs"""
        auth_token = self.get_vrops_token()
        api_url = "/suite-api/api/resources?resourceKind=VirtualMachine"
        headers = {
            "Accept": "application/json",
            "Authorization": "vRealizeOpsToken {}".format(auth_token),
        }
        resource_list = []

        resp = requests.get(self.vrops_site + api_url, verify=False, headers=headers)

        if resp.status_code != 200:
            log.error(
                "Failed to get resource list from vROPS: {} {}".format(
                    resp.status_code, resp.content
                )
            )
            return resource_list

        try:
            resp_data = json.loads(resp.content.decode("utf-8"))
            if resp_data.get("resourceList") is not None:
                resource_list = resp_data.get("resourceList")

        except Exception as exp:
            log.error(
                "get_vm_resource_id: Error in parsing {}\n{}".format(
                    exp, traceback.format_exc()
                )
            )

        return resource_list

    def get_metrics(self, metrics_list=[]):
        monitoring_keys = {}
        vdus = {}
        # Collect the names of all the metrics we need to query
        for metric in metrics_list:
            metric_name = metric["metric"]
            if metric_name not in METRIC_MAPPINGS:
                log.debug(f"Metric {metric_name} not supported, ignoring")
                continue
            monitoring_keys[metric_name] = METRIC_MAPPINGS[metric_name]
            vrops_id = metric["vrops_id"]
            vdus[vrops_id] = 1

        metrics = []
        # Make a query for only the stats we have been asked for
        stats_key = ""
        for stat in monitoring_keys.values():
            stats_key += "&statKey={}".format(stat)

        # And only ask for the resource ids that we are interested in
        resource_ids = ""
        for key in vdus.keys():
            resource_ids += "&resourceId={}".format(key)

        try:
            # Now we can make a single call to vROPS to collect all relevant metrics for resources we need to monitor
            api_url = (
                "/suite-api/api/resources/stats?IntervalType=MINUTES&IntervalCount=1"
                "&rollUpType=MAX&currentOnly=true{}{}".format(stats_key, resource_ids)
            )

            auth_token = self.get_vrops_token()
            headers = {
                "Accept": "application/json",
                "Authorization": "vRealizeOpsToken {}".format(auth_token),
            }

            resp = requests.get(
                self.vrops_site + api_url, verify=False, headers=headers
            )

            if resp.status_code != 200:
                log.error(
                    f"Failed to get Metrics data from vROPS for {resp.status_code} {resp.content}"
                )
                return []
            m_data = json.loads(resp.content.decode("utf-8"))
            if "values" not in m_data:
                return metrics

            statistics = m_data["values"]
            for vdu_stat in statistics:
                vrops_id = vdu_stat["resourceId"]
                log.info(f"vrops_id: {vrops_id}")
                for item in vdu_stat["stat-list"]["stat"]:
                    reported_metric = item["statKey"]["key"]
                    if reported_metric not in METRIC_MAPPINGS.values():
                        continue

                    # Convert the vROPS metric name back to OSM key
                    metric_name = list(METRIC_MAPPINGS.keys())[
                        list(METRIC_MAPPINGS.values()).index(reported_metric)
                    ]
                    if metric_name in monitoring_keys.keys():
                        metric_value = item["data"][-1]
                        if metric_name in METRIC_MULTIPLIERS:
                            metric_value *= METRIC_MULTIPLIERS[metric_name]
                        log.info(f"  {metric_name} ({reported_metric}): {metric_value}")

                        # Find the associated metric in requested list
                        for item in metrics_list:
                            if (
                                item["vrops_id"] == vrops_id
                                and item["metric"] == metric_name
                            ):
                                metric = item
                                metric["value"] = metric_value
                                metrics.append(metric)
                                break

        except Exception as exp:
            log.error(
                "Exception while parsing metrics data from vROPS {}\n{}".format(
                    exp, traceback.format_exc()
                )
            )

        return metrics
