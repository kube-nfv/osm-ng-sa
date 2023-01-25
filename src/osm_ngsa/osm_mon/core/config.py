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
"""Global configuration managed by environment variables."""

import logging
import os

import pkg_resources
import yaml

logger = logging.getLogger(__name__)


class Config:
    def __init__(self, config_file: str = ""):
        self.conf = {}
        self._read_config_file(config_file)
        self._read_env()

    def _read_config_file(self, config_file):
        if not config_file:
            path = "config.yaml"
            config_file = pkg_resources.resource_filename(__name__, path)
        with open(config_file) as f:
            self.conf = yaml.safe_load(f)

    def get(self, section, field=None):
        if not field:
            return self.conf[section]
        return self.conf[section].get(field)

    def set(self, section, field, value):
        if section not in self.conf:
            self.conf[section] = {}
        self.conf[section][field] = value

    def _read_env(self):
        for env in os.environ:
            if not env.startswith("OSMMON_"):
                continue
            elements = env.lower().split("_")
            if len(elements) < 3:
                logger.warning(
                    "Environment variable %s=%s does not comply with required format. Section and/or field missing.",
                    env,
                    os.getenv(env),
                )
                continue
            section = elements[1]
            field = "_".join(elements[2:])
            value = os.getenv(env)
            if section not in self.conf:
                self.conf[section] = {}
            self.conf[section][field] = value
