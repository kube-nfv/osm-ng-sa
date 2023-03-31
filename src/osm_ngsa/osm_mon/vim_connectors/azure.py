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
import logging
from typing import Dict, List

from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.profiles import ProfileDefinition
from osm_mon.vim_connectors.base_vim import VIMConnector


log = logging.getLogger(__name__)


class AzureCollector(VIMConnector):
    # Translate azure provisioning state to OSM provision state.
    # The first three ones are the transitional status once a user initiated
    # action has been requested. Once the operation is complete, it will
    # transition into the states Succeeded or Failed
    # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/states-lifecycle
    provision_state2osm = {
        "Creating": "BUILD",
        "Updating": "BUILD",
        "Deleting": "INACTIVE",
        "Succeeded": "ACTIVE",
        "Failed": "ERROR",
    }

    # Translate azure power state to OSM provision state
    power_state2osm = {
        "starting": "INACTIVE",
        "running": "ACTIVE",
        "stopping": "INACTIVE",
        "stopped": "INACTIVE",
        "unknown": "OTHER",
        "deallocated": "BUILD",
        "deallocating": "BUILD",
    }

    AZURE_COMPUTE_MGMT_CLIENT_API_VERSION = "2021-03-01"
    AZURE_COMPUTE_MGMT_PROFILE_TAG = "azure.mgmt.compute.ComputeManagementClient"
    AZURE_COMPUTE_MGMT_PROFILE = ProfileDefinition(
        {
            AZURE_COMPUTE_MGMT_PROFILE_TAG: {
                None: AZURE_COMPUTE_MGMT_CLIENT_API_VERSION,
                "availability_sets": "2020-12-01",
                "dedicated_host_groups": "2020-12-01",
                "dedicated_hosts": "2020-12-01",
                "disk_accesses": "2020-12-01",
                "disk_encryption_sets": "2020-12-01",
                "disk_restore_point": "2020-12-01",
                "disks": "2020-12-01",
                "galleries": "2020-09-30",
                "gallery_application_versions": "2020-09-30",
                "gallery_applications": "2020-09-30",
                "gallery_image_versions": "2020-09-30",
                "gallery_images": "2020-09-30",
                "gallery_sharing_profile": "2020-09-30",
                "images": "2020-12-01",
                "log_analytics": "2020-12-01",
                "operations": "2020-12-01",
                "proximity_placement_groups": "2020-12-01",
                "resource_skus": "2019-04-01",
                "shared_galleries": "2020-09-30",
                "shared_gallery_image_versions": "2020-09-30",
                "shared_gallery_images": "2020-09-30",
                "snapshots": "2020-12-01",
                "ssh_public_keys": "2020-12-01",
                "usage": "2020-12-01",
                "virtual_machine_extension_images": "2020-12-01",
                "virtual_machine_extensions": "2020-12-01",
                "virtual_machine_images": "2020-12-01",
                "virtual_machine_images_edge_zone": "2020-12-01",
                "virtual_machine_run_commands": "2020-12-01",
                "virtual_machine_scale_set_extensions": "2020-12-01",
                "virtual_machine_scale_set_rolling_upgrades": "2020-12-01",
                "virtual_machine_scale_set_vm_extensions": "2020-12-01",
                "virtual_machine_scale_set_vm_run_commands": "2020-12-01",
                "virtual_machine_scale_set_vms": "2020-12-01",
                "virtual_machine_scale_sets": "2020-12-01",
                "virtual_machine_sizes": "2020-12-01",
                "virtual_machines": "2020-12-01",
            }
        },
        AZURE_COMPUTE_MGMT_PROFILE_TAG + " osm",
    )

    def __init__(self, vim_account: Dict):
        self.vim_account = vim_account
        self.reload_client = True
        logger = logging.getLogger("azure")
        logger.setLevel(logging.ERROR)
        # Store config to create azure subscription later
        self._config = {
            "user": vim_account["vim_user"],
            "passwd": vim_account["vim_password"],
            "tenant": vim_account["vim_tenant_name"],
        }

        # SUBSCRIPTION
        config = vim_account["config"]
        if "subscription_id" in config:
            self._config["subscription_id"] = config.get("subscription_id")
            log.info("Subscription: %s", self._config["subscription_id"])
        else:
            log.error("Subscription not specified")
            return

        # RESOURCE_GROUP
        if "resource_group" in config:
            self.resource_group = config.get("resource_group")
        else:
            log.error("Azure resource_group is not specified at config")
            return

    def _reload_connection(self):
        if self.reload_client:
            log.debug("reloading azure client")
            try:
                self.credentials = ClientSecretCredential(
                    client_id=self._config["user"],
                    client_secret=self._config["passwd"],
                    tenant_id=self._config["tenant"],
                )
                self.conn_compute = ComputeManagementClient(
                    self.credentials,
                    self._config["subscription_id"],
                    profile=self.AZURE_COMPUTE_MGMT_PROFILE,
                )
                # Set to client created
                self.reload_client = False
            except Exception as e:
                log.error(e)

    def collect_servers_status(self) -> List[Dict]:
        servers = []
        log.debug("collect_servers_status")
        self._reload_connection()
        try:
            for vm in self.conn_compute.virtual_machines.list(self.resource_group):
                id = vm.id
                array = id.split("/")
                name = array[-1]
                status = self.provision_state2osm.get(vm.provisioning_state, "OTHER")
                if vm.provisioning_state == "Succeeded":
                    # check if machine is running or stopped
                    instance_view = self.conn_compute.virtual_machines.instance_view(
                        self.resource_group, name
                    )
                    for status in instance_view.statuses:
                        splitted_status = status.code.split("/")
                        if (
                            len(splitted_status) == 2
                            and splitted_status[0] == "PowerState"
                        ):
                            status = self.power_state2osm.get(
                                splitted_status[1], "OTHER"
                            )
                # log.info(f'id: {id}, name: {name}, status: {status}')
                vm = {
                    "id": id,
                    "name": name,
                    "status": (1 if (status == "ACTIVE") else 0),
                }
                servers.append(vm)
        except Exception as e:
            log.error(e)
        return servers

    def is_vim_ok(self) -> bool:
        status = False
        self.reload_client = True
        try:
            self._reload_connection()
            status = True
        except Exception as e:
            log.error(e)
        return status
