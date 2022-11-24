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

from keystoneauth1 import session
from keystoneauth1.identity import v3
from novaclient import client as nova_client
from osm_mon.vim_connectors.base_vim import VIMConnector

log = logging.getLogger(__name__)


class CertificateNotCreated(Exception):
    pass


class OpenStackCollector(VIMConnector):
    def __init__(self, vim_account: Dict):
        log.info("__init__")
        self.vim_account = vim_account
        self.vim_session = None
        self.vim_session = self._get_session(vim_account)
        self.nova = self._build_nova_client()

    def _get_session(self, creds: Dict):
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

    def _build_nova_client(self) -> nova_client.Client:
        return nova_client.Client("2", session=self.vim_session, timeout=10)

    def collect_servers_status(self) -> List[Dict]:
        log.info("collect_servers_status")
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
        try:
            self.nova.servers.list()
            return True
        except Exception as e:
            log.warning("VIM status is not OK: %s" % e)
            return False
