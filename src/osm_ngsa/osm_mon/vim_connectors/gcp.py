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
from typing import Dict, List

from google.oauth2 import service_account
import googleapiclient.discovery
from osm_mon.vim_connectors.base_vim import VIMConnector

log = logging.getLogger(__name__)


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
                        "id": server["id"],
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
