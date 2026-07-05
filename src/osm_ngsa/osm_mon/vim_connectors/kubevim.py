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
"""VIM connector for kube-vim.

kube-vim exposes no pollable performance-management API. Instead, its metrics
(and the metrics of the backends it manages) live in Prometheus:

  * kube-vim publishes correlation gauges ``kubevim_compute_info`` /
    ``kubevim_vnic_info`` (value always 1) that map ETSI ids to KubeVirt / VF
    native labels.
  * KubeVirt (virt-handler) exports per-VM counters ``kubevirt_vmi_*``.
  * The SR-IOV network metrics exporter exports per-VF counters ``sriov_vf_*``.

This collector queries that Prometheus and joins the backend counters back to
the ETSI compute id (``vm_id`` in OSM == kube-vim ``compute_id``), producing the
normalised OSM metric set. Network totals are the union of the KubeVirt (bridge/
virtio) datapath and the SR-IOV VF datapath, because VF traffic bypasses KubeVirt
and is invisible to ``kubevirt_vmi_network_*``.
"""

import logging
from typing import Dict, List

from osm_ngsa.osm_mon.vim_connectors.base_vim import VIMConnector
from prometheus_api_client import PrometheusConnect as prometheus_client

log = logging.getLogger(__name__)

# In-cluster kube-prometheus-stack service; override per VIM via
# vim_account["config"]["prometheus_url"].
DEFAULT_PROMETHEUS_URL = "http://monitoring-kube-prometheus-prometheus.monitoring:9090"

# Rate window; the DAG runs every 5 min and the exporters are scraped at 30s.
RATE_INTERVAL = "5m"

# Join a per-VM ("name") expression to the ETSI compute id. ``kubevim_compute_info``
# maps compute_id -> compute_name, and compute_name == the kubevirt_vmi "name" label.
_COMPUTE_JOIN = (
    '{expr} * on(name) group_left(compute_id) '
    'label_replace(kubevim_compute_info, "name", "$1", "compute_name", "(.+)")'
)

# Join a per-VF ("pciAddr") expression to the ETSI compute id via the pci_address
# label that kube-vim stamps on SR-IOV vNICs.
_SRIOV_JOIN = (
    'sum by (compute_id) ({expr} * on(pciAddr) group_left(compute_id) '
    'label_replace(kubevim_vnic_info{{type="TYPE_VIRTUAL_NIC_SRIOV"}}, '
    '"pciAddr", "$1", "pci_address", "(.+)"))'
)

# Metrics that come solely from the KubeVirt per-VM counters. Each template, once
# joined, yields a vector carrying the ``compute_id`` label.
COMPUTE_METRIC_QUERIES = {
    # CPU utilisation as a percentage of the VM's allocated vCPUs.
    "cpu_utilization": _COMPUTE_JOIN.format(
        expr=(
            "100 * ("
            f"sum by (name) (rate(kubevirt_vmi_cpu_usage_seconds_total[{RATE_INTERVAL}])) "
            "/ count by (name) (count by (name, id) (kubevirt_vmi_vcpu_seconds_total))"
            ")"
        )
    ),
    # RAM used by the guest, in MB (requires the qemu guest agent for usable_bytes).
    "average_memory_utilization": _COMPUTE_JOIN.format(
        expr=(
            "(sum by (name) (kubevirt_vmi_memory_available_bytes) "
            "- sum by (name) (kubevirt_vmi_memory_usable_bytes)) / 1048576"
        )
    ),
    "disk_read_ops": _COMPUTE_JOIN.format(
        expr=f"sum by (name) (rate(kubevirt_vmi_storage_iops_read_total[{RATE_INTERVAL}]))"
    ),
    "disk_write_ops": _COMPUTE_JOIN.format(
        expr=f"sum by (name) (rate(kubevirt_vmi_storage_iops_write_total[{RATE_INTERVAL}]))"
    ),
    "disk_read_bytes": _COMPUTE_JOIN.format(
        expr=f"sum by (name) (rate(kubevirt_vmi_storage_read_traffic_bytes_total[{RATE_INTERVAL}]))"
    ),
    "disk_write_bytes": _COMPUTE_JOIN.format(
        expr=f"sum by (name) (rate(kubevirt_vmi_storage_write_traffic_bytes_total[{RATE_INTERVAL}]))"
    ),
}

# Network metrics are the sum of the bridge/virtio datapath (KubeVirt) and the
# SR-IOV VF datapath (sriov exporter). Both queries yield vectors keyed by
# compute_id; the collector adds them per compute_id.
NETWORK_METRIC_QUERIES = {
    "packets_received": {
        "kubevirt": "kubevirt_vmi_network_receive_packets_total",
        "sriov": "sriov_vf_rx_packets",
    },
    "packets_sent": {
        "kubevirt": "kubevirt_vmi_network_transmit_packets_total",
        "sriov": "sriov_vf_tx_packets",
    },
    "packets_in_dropped": {
        "kubevirt": "kubevirt_vmi_network_receive_packets_dropped_total",
        "sriov": "sriov_vf_rx_dropped",
    },
    "packets_out_dropped": {
        "kubevirt": "kubevirt_vmi_network_transmit_packets_dropped_total",
        "sriov": "sriov_vf_tx_dropped",
    },
}


class KubevimCollector(VIMConnector):
    def __init__(self, vim_account: Dict):
        log.debug("__init__")
        self.vim_account = vim_account
        config = vim_account.get("config", {}) or {}
        self.prometheus_url = config.get("prometheus_url", DEFAULT_PROMETHEUS_URL)
        self.client = prometheus_client(self.prometheus_url, disable_ssl=True)

    def _query(self, promql: str) -> List[Dict]:
        try:
            return self.client.custom_query(query=promql)
        except Exception as e:
            log.error(f"Prometheus query failed ({promql}): {e}")
            return []

    def _query_by_compute_id(self, promql: str) -> Dict[str, float]:
        """Run a query whose result carries a ``compute_id`` label and return
        {compute_id: value}."""
        result = {}
        for series in self._query(promql):
            compute_id = series.get("metric", {}).get("compute_id")
            if not compute_id:
                continue
            try:
                result[compute_id] = float(series["value"][1])
            except (KeyError, IndexError, ValueError):
                continue
        return result

    def is_vim_ok(self) -> bool:
        # kube-vim is up and being scraped if it publishes its build-info gauge.
        return bool(self._query("kubevim_build_info"))

    def collect_servers_status(self) -> List[Dict]:
        # NOTE: kubevim_compute_info has no per-VIM label, so this returns every
        # compute known to this Prometheus. compute_id is globally unique, so in a
        # single-kube-vim deployment this maps 1:1 to the VIM's servers.
        servers = []
        for series in self._query("kubevim_compute_info"):
            m = series.get("metric", {})
            compute_id = m.get("compute_id")
            if not compute_id:
                continue
            servers.append(
                {
                    "id": compute_id,
                    "name": m.get("compute_name", ""),
                    "status": 1 if m.get("running_state") == "RUNNING" else 0,
                }
            )
        return servers

    def collect_metrics(self, metric_list: List[Dict]) -> List[Dict]:
        log.debug("collect_metrics")
        if not metric_list:
            return []

        # Only query the metric families actually requested.
        requested = {m["metric"] for m in metric_list}

        # {metric_name: {compute_id: value}}
        values: Dict[str, Dict[str, float]] = {}

        for name in requested & COMPUTE_METRIC_QUERIES.keys():
            values[name] = self._query_by_compute_id(COMPUTE_METRIC_QUERIES[name])

        for name in requested & NETWORK_METRIC_QUERIES.keys():
            sources = NETWORK_METRIC_QUERIES[name]
            bridge = self._query_by_compute_id(
                _COMPUTE_JOIN.format(
                    expr=f"sum by (name) (rate({sources['kubevirt']}[{RATE_INTERVAL}]))"
                )
            )
            sriov = self._query_by_compute_id(
                _SRIOV_JOIN.format(
                    expr=f"rate({sources['sriov']}[{RATE_INTERVAL}])"
                )
            )
            merged = dict(bridge)
            for compute_id, val in sriov.items():
                merged[compute_id] = merged.get(compute_id, 0.0) + val
            values[name] = merged

        results = []
        for metric in metric_list:
            name = metric["metric"]
            vm_id = metric["vm_id"]
            value = values.get(name, {}).get(vm_id)
            if value is None:
                continue
            metric["value"] = value
            results.append(metric)

        log.info(f"Collected {len(results)}/{len(metric_list)} metrics")
        return results
