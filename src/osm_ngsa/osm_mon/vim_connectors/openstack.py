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
from enum import Enum
import logging
import time
from typing import Dict, List

from ceilometerclient import client as ceilometer_client
from ceilometerclient.exc import HTTPException
import gnocchiclient.exceptions
from gnocchiclient.v1 import client as gnocchi_client
from keystoneauth1 import session
from keystoneauth1.exceptions.catalog import EndpointNotFound
from keystoneauth1.identity import v3
from keystoneclient.v3 import client as keystone_client
from novaclient import client as nova_client
from osm_ngsa.osm_mon.vim_connectors.base_vim import VIMConnector
from osm_ngsa.osm_mon.vim_connectors.vrops_helper import vROPS_Helper
from prometheus_api_client import PrometheusConnect as prometheus_client

log = logging.getLogger(__name__)

METRIC_MULTIPLIERS = {"cpu": 0.0000001}

METRIC_AGGREGATORS = {"cpu": "rate:mean"}

INTERFACE_METRICS = [
    "packets_in_dropped",
    "packets_out_dropped",
    "packets_received",
    "packets_sent",
]

INSTANCE_DISK = [
    "disk_read_ops",
    "disk_write_ops",
    "disk_read_bytes",
    "disk_write_bytes",
]

METRIC_MAPPINGS = {
    "average_memory_utilization": "memory.usage",
    "disk_read_ops": "disk.device.read.requests",
    "disk_write_ops": "disk.device.write.requests",
    "disk_read_bytes": "disk.device.read.bytes",
    "disk_write_bytes": "disk.device.write.bytes",
    "packets_in_dropped": "network.outgoing.packets.drop",
    "packets_out_dropped": "network.incoming.packets.drop",
    "packets_received": "network.incoming.packets",
    "packets_sent": "network.outgoing.packets",
    "cpu_utilization": "cpu",
}

METRIC_MAPPINGS_FOR_PROMETHEUS_TSBD = {
    "cpu_utilization": "cpu",
    "average_memory_utilization": "memory_usage",
    "disk_read_ops": "disk_device_read_requests",
    "disk_write_ops": "disk_device_write_requests",
    "disk_read_bytes": "disk_device_read_bytes",
    "disk_write_bytes": "disk_device_write_bytes",
    "packets_in_dropped": "network_incoming_packets_drop",
    "packets_out_dropped": "network_outgoing_packets_drop",
    "packets_received": "network_incoming_packets",
    "packets_sent": "network_outgoing_packets",
}


class MetricType(Enum):
    INSTANCE = "instance"
    INTERFACE_ALL = "interface_all"
    INTERFACE_ONE = "interface_one"
    INSTANCEDISK = "instancedisk"


class CertificateNotCreated(Exception):
    pass


class OpenStackCollector(VIMConnector):
    def __init__(self, vim_account: Dict):
        log.debug("__init__")
        self.vim_account = vim_account
        self.vim_session = None
        self.vim_session = self._get_session(vim_account)
        self.nova = self._build_nova_client()
        self.backend = self._get_backend(vim_account, self.vim_session)

    def _get_session(self, creds: Dict):
        log.debug("_get_session")
        verify_ssl = True
        project_domain_name = "Default"
        user_domain_name = "Default"
        try:
            if "config" in creds:
                vim_config = creds["config"]
                if "insecure" in vim_config and vim_config["insecure"]:
                    verify_ssl = False
                if "ca_cert" in vim_config:
                    verify_ssl = vim_config["ca_cert"]
                elif "ca_cert_content" in vim_config:
                    # vim_config = self._create_file_cert(vim_config, creds["_id"])
                    verify_ssl = vim_config["ca_cert"]
                if "project_domain_name" in vim_config:
                    project_domain_name = vim_config["project_domain_name"]
                if "user_domain_name" in vim_config:
                    user_domain_name = vim_config["user_domain_name"]
            auth = v3.Password(
                auth_url=creds["vim_url"],
                username=creds["vim_user"],
                password=creds["vim_password"],
                project_name=creds["vim_tenant_name"],
                project_domain_name=project_domain_name,
                user_domain_name=user_domain_name,
            )
            return session.Session(auth=auth, verify=verify_ssl, timeout=10)
        except CertificateNotCreated as e:
            log.error(e)

    def _determine_backend(self):
        keystone = keystone_client.Client(session=self.vim_session)
        # Obtain the service catalog from OpenStack
        service_catalog = keystone.services.list()
        log.info(f"Openstack service catalog: {service_catalog}")
        # Convert into a dictionary
        services = {service.name: service.type for service in service_catalog}
        log.info(f"Openstack services: {services}")
        if "prometheus" in services:
            log.info("Using Prometheus backend")
            return "prometheus"
        elif "gnocchi" in services:
            log.info("Using Gnocchi backend")
            return "gnocchi"
        elif "ceilometer" in services:
            log.info("Using Ceilometer backend")
            return "ceilometer"
        else:
            log.info("No suitable backend found")
            return None

    def _get_backend(self, vim_account: dict, vim_session: object):
        openstack_metrics_backend = self._determine_backend()
        log.info(f"openstack_metrics_backend: {openstack_metrics_backend}")

        # Priority 1. If prometheus-config, use Prometheus backend
        log.info(f"vim_account: {vim_account}")
        if vim_account.get("prometheus-config"):
            try:
                tsbd = PrometheusTSBDBackend(vim_account)
                log.info("Using prometheustsbd backend to collect metric")
                return tsbd
            except Exception as e:
                log.error(f"Can't create prometheus client, {e}")
                return None

        # Extract config to avoid repetitive checks
        vim_config = vim_account.get("config", {})

        # Priority 2. If the VIM is VIO and there is VROPS, use VROPS backend
        vim_type = vim_config.get("vim_type", "").lower()
        log.info(f"vim_type: {vim_type}")
        if vim_type == "vio" and "vrops_site" in vim_config:
            try:
                log.info("Using vROPS backend to collect metric")
                vrops = VropsBackend(vim_account)
                return vrops
            except Exception as e:
                log.error(f"Can't create vROPS client, {e}")
                return None

        # Priority 3: try Gnocchi
        try:
            gnocchi = GnocchiBackend(vim_account, vim_session)
            gnocchi.client.metric.list(limit=1)
            log.info("Using gnocchi backend to collect metric")
            return gnocchi
        except (gnocchiclient.exceptions.ClientException, EndpointNotFound) as e:
            log.warning(f"Gnocchi not available: {e}")

        # Priority 4: try Ceilometer
        try:
            ceilometer = CeilometerBackend(vim_account, vim_session)
            ceilometer.client.capabilities.get()
            log.info("Using ceilometer backend to collect metric")
            return ceilometer
        except (HTTPException, EndpointNotFound) as e:
            log.warning(f"Ceilometer not available: {e}")

        log.error("Failed to find a proper backend")
        return None

    def _build_nova_client(self) -> nova_client.Client:
        return nova_client.Client("2", session=self.vim_session, timeout=10)

    def _build_gnocchi_client(self) -> gnocchi_client.Client:
        return gnocchi_client.Client(session=self.vim_session)

    def collect_servers_status(self) -> List[Dict]:
        log.debug("collect_servers_status")
        servers = []
        for server in self.nova.servers.list(detailed=True):
            vm = {
                "id": server.id,
                "name": server.name,
                "status": (0 if (server.status == "ERROR") else 1),
            }
            servers.append(vm)
        return servers

    def is_vim_ok(self) -> bool:
        log.debug("is_vim_ok")
        try:
            self.nova.servers.list()
            return True
        except Exception as e:
            log.warning("VIM status is not OK: %s" % e)
            return False

    def _get_metric_type(self, metric_name: str) -> MetricType:
        if metric_name not in INTERFACE_METRICS:
            if metric_name not in INSTANCE_DISK:
                return MetricType.INSTANCE
            else:
                return MetricType.INSTANCEDISK
        else:
            return MetricType.INTERFACE_ALL

    def collect_metrics(self, metric_list: List[Dict]) -> List[Dict]:
        log.debug("collect_metrics")
        if not self.backend:
            log.error("Undefined backend")
            return []

        if type(self.backend) is VropsBackend:
            log.info("Using vROPS as backend")
            return self.backend.collect_metrics(metric_list)

        metric_results = []
        for metric in metric_list:
            server = metric["vm_id"]
            metric_name = metric["metric"]
            # metric_type is only relevant for Gnocchi and Ceilometer
            metric_type = self._get_metric_type(metric_name)
            try:
                backend_metric_name = self.backend.map_metric(metric_name)
            except KeyError:
                continue
            log.info(f"Collecting metric {backend_metric_name} for {server}")
            try:
                value = self.backend.collect_metric(
                    metric_type, backend_metric_name, server
                )
                if value is not None:
                    log.info(f"value: {value}")
                    metric["value"] = value
                    metric_results.append(metric)
                else:
                    log.info("metric value is empty")
            except Exception as e:
                log.error("Error in metric collection: %s" % e)
        return metric_results


class OpenstackBackend:
    def collect_metric(
        self, metric_type: MetricType, metric_name: str, resource_id: str
    ):
        pass

    def collect_metrics(self, metrics_list: List[Dict]):
        pass

    def map_metric(self, metric_name: str):
        return METRIC_MAPPINGS[metric_name]


class PrometheusTSBDBackend(OpenstackBackend):
    def __init__(self, vim_account: dict):
        self.map = self._build_map(vim_account)
        self.cred = vim_account["prometheus-config"].get("prometheus-cred")
        self.client = self._build_prometheus_client(
            vim_account["prometheus-config"]["prometheus-url"]
        )

    def _build_prometheus_client(self, url: str) -> prometheus_client:
        return prometheus_client(url, disable_ssl=True)

    def _build_map(self, vim_account: dict) -> dict:
        custom_map = METRIC_MAPPINGS_FOR_PROMETHEUS_TSBD
        if "prometheus-map" in vim_account["prometheus-config"]:
            custom_map.update(vim_account["prometheus-config"]["prometheus-map"])
        return custom_map

    def collect_metric(
        self, metric_type: MetricType, metric_name: str, resource_id: str
    ):
        log.info(f"Collecting metric {metric_name} from Prometheus for {resource_id}")
        metric = self.query_metric(metric_name, resource_id)
        # From the timeseries returned by Prometheus, we take the second element in the array
        # corresponding to the metric value.
        # 0: timestamp
        # 1: metric value
        return metric["value"][1] if metric else None

    def map_metric(self, metric_name: str):
        return self.map[metric_name]

    def query_metric(self, metric_name, resource_id=None):
        log.info(f"Querying metric {metric_name} for {resource_id}")
        metrics = self.client.get_current_metric_value(metric_name=metric_name)
        log.debug(f"Global result of querying metric: {metrics}")
        if resource_id:
            log.info(f"Getting the metric for the resource_id: {resource_id}")
            metric = next(
                # TODO: The label to identify the metric should be standard or read from VIM config
                filter(lambda x: resource_id in x["metric"]["resource_id"], metrics)
            )
            log.debug(f"Resource metric {metric_name} for {resource_id}: {metric}")
            return metric
        return metrics


class GnocchiBackend(OpenstackBackend):
    def __init__(self, vim_account: dict, vim_session: object):
        self.client = self._build_gnocchi_client(vim_account, vim_session)

    def _build_gnocchi_client(
        self, vim_account: dict, vim_session: object
    ) -> gnocchi_client.Client:
        return gnocchi_client.Client(session=vim_session)

    def collect_metric(
        self, metric_type: MetricType, metric_name: str, resource_id: str
    ):
        if metric_type == MetricType.INTERFACE_ALL:
            return self._collect_interface_all_metric(metric_name, resource_id)

        elif metric_type == MetricType.INSTANCE:
            return self._collect_instance_metric(metric_name, resource_id)

        elif metric_type == MetricType.INSTANCEDISK:
            return self._collect_instance_disk_metric(metric_name, resource_id)

        else:
            raise Exception("Unknown metric type %s" % metric_type.value)

    def _collect_interface_all_metric(self, openstack_metric_name, resource_id):
        total_measure = None
        interfaces = self.client.resource.search(
            resource_type="instance_network_interface",
            query={"=": {"instance_id": resource_id}},
        )
        for interface in interfaces:
            try:
                measures = self.client.metric.get_measures(
                    openstack_metric_name, resource_id=interface["id"], limit=1
                )
                if measures:
                    if not total_measure:
                        total_measure = 0.0
                    total_measure += measures[-1][2]
            except (gnocchiclient.exceptions.NotFound, TypeError) as e:
                # Gnocchi in some Openstack versions raise TypeError instead of NotFound
                log.debug(
                    "No metric %s found for interface %s: %s",
                    openstack_metric_name,
                    interface["id"],
                    e,
                )
        return total_measure

    def _collect_instance_disk_metric(self, openstack_metric_name, resource_id):
        value = None
        instances = self.client.resource.search(
            resource_type="instance_disk",
            query={"=": {"instance_id": resource_id}},
        )
        for instance in instances:
            try:
                measures = self.client.metric.get_measures(
                    openstack_metric_name, resource_id=instance["id"], limit=1
                )
                if measures:
                    value = measures[-1][2]

            except gnocchiclient.exceptions.NotFound as e:
                log.debug(
                    "No metric %s found for instance disk %s: %s",
                    openstack_metric_name,
                    instance["id"],
                    e,
                )
        return value

    def _collect_instance_metric(self, openstack_metric_name, resource_id):
        value = None
        try:
            aggregation = METRIC_AGGREGATORS.get(openstack_metric_name)

            try:
                measures = self.client.metric.get_measures(
                    openstack_metric_name,
                    aggregation=aggregation,
                    start=time.time() - 1200,
                    resource_id=resource_id,
                )
                if measures:
                    value = measures[-1][2]
            except (
                gnocchiclient.exceptions.NotFound,
                gnocchiclient.exceptions.BadRequest,
                TypeError,
            ) as e:
                # CPU metric in previous Openstack versions do not support rate:mean aggregation method
                # Gnocchi in some Openstack versions raise TypeError instead of NotFound or BadRequest
                if openstack_metric_name == "cpu":
                    log.debug(
                        "No metric %s found for instance %s: %s",
                        openstack_metric_name,
                        resource_id,
                        e,
                    )
                    log.info(
                        "Retrying to get metric %s for instance %s without aggregation",
                        openstack_metric_name,
                        resource_id,
                    )
                    measures = self.client.metric.get_measures(
                        openstack_metric_name, resource_id=resource_id, limit=1
                    )
                else:
                    raise e
                # measures[-1] is the last measure
                # measures[-2] is the previous measure
                # measures[x][2] is the value of the metric
                if measures and len(measures) >= 2:
                    value = measures[-1][2] - measures[-2][2]
            if value:
                # measures[-1][0] is the time of the reporting interval
                # measures[-1][1] is the duration of the reporting interval
                if aggregation:
                    # If this is an aggregate, we need to divide the total over the reported time period.
                    # Even if the aggregation method is not supported by Openstack, the code will execute it
                    # because aggregation is specified in METRIC_AGGREGATORS
                    value = value / measures[-1][1]
                if openstack_metric_name in METRIC_MULTIPLIERS:
                    value = value * METRIC_MULTIPLIERS[openstack_metric_name]
        except gnocchiclient.exceptions.NotFound as e:
            log.debug(
                "No metric %s found for instance %s: %s",
                openstack_metric_name,
                resource_id,
                e,
            )
        return value


class CeilometerBackend(OpenstackBackend):
    def __init__(self, vim_account: dict, vim_session: object):
        self.client = self._build_ceilometer_client(vim_account, vim_session)

    def _build_ceilometer_client(
        self, vim_account: dict, vim_session: object
    ) -> ceilometer_client.Client:
        return ceilometer_client.Client("2", session=vim_session)

    def collect_metric(
        self, metric_type: MetricType, metric_name: str, resource_id: str
    ):
        if metric_type != MetricType.INSTANCE:
            raise NotImplementedError(
                "Ceilometer backend only support instance metrics"
            )
        measures = self.client.samples.list(
            meter_name=metric_name,
            limit=1,
            q=[{"field": "resource_id", "op": "eq", "value": resource_id}],
        )
        return measures[0].counter_volume if measures else None


class VropsBackend(OpenstackBackend):
    def __init__(self, vim_account: dict):
        self.vrops = vROPS_Helper(
            vrops_site=vim_account["config"]["vrops_site"],
            vrops_user=vim_account["config"]["vrops_user"],
            vrops_password=vim_account["config"]["vrops_password"],
        )

    def collect_metrics(self, metrics_list: List[Dict]):
        # Fetch the list of all known resources from vROPS.
        resource_list = self.vrops.get_vm_resource_list_from_vrops()

        vdu_mappings = {}
        extended_metrics = []
        for metric in metrics_list:
            vim_id = metric["vm_id"]
            # Map the vROPS instance id to the vim-id so we can look it up.
            for resource in resource_list:
                for resourceIdentifier in resource["resourceKey"][
                    "resourceIdentifiers"
                ]:
                    if (
                        resourceIdentifier["identifierType"]["name"]
                        == "VMEntityInstanceUUID"
                    ):
                        if resourceIdentifier["value"] != vim_id:
                            continue
                        vdu_mappings[vim_id] = resource["identifier"]
            if vim_id in vdu_mappings:
                metric["vrops_id"] = vdu_mappings[vim_id]
                extended_metrics.append(metric)

        if len(extended_metrics) != 0:
            return self.vrops.get_metrics(extended_metrics)
        else:
            return []
