# Copyright 2018 Whitestack, LLC
# *************************************************************

# This file is part of OSM Monitoring module
# All Rights Reserved to Whitestack, LLC

# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at

#         http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# For those usages not covered by the Apache License, Version 2.0 please
# contact: bdiaz@whitestack.com or glavado@whitestack.com
##
import logging
from typing import Dict

from osm_ngsa.osm_mon.sdnc_connectors.base_sdnc import SDNCConnector
import requests
from requests.auth import HTTPBasicAuth

log = logging.getLogger(__name__)


class OnosInfraCollector(SDNCConnector):
    def __init__(self, sdnc_account: Dict):
        self.sdnc_account = sdnc_account

    def _obtain_url(self):
        url = self.sdnc_account.get("url")
        if url:
            return url
        else:
            if not self.sdnc_account.get("ip") or not self.sdnc_account.get("port"):
                raise Exception("You must provide a URL to contact the SDN Controller")
            else:
                return "http://{}:{}/onos/v1/devices".format(
                    self.sdnc_account["ip"], self.sdnc_account["port"]
                )

    def is_sdnc_ok(self) -> bool:
        try:
            url = self._obtain_url()
            user = self.sdnc_account["user"]
            password = self.sdnc_account["password"]

            requests.get(url, auth=HTTPBasicAuth(user, password))
            return True
        except Exception:
            log.exception("SDNC status is not OK!")
            return False
