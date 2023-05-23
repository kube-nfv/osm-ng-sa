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
from osm_common import dbmemory, dbmongo
from osm_mon.core.config import Config


class CommonDbClient:
    def __init__(self, config: Config):
        if config.get("database", "driver") == "mongo":
            self.common_db = dbmongo.DbMongo()
        elif config.get("database", "driver") == "memory":
            self.common_db = dbmemory.DbMemory()
        else:
            raise Exception(
                "Unknown database driver {}".format(config.get("section", "driver"))
            )
        self.common_db.db_connect(config.get("database"))

    def get_vnfr(self, nsr_id: str, member_index: str):
        vnfr = self.common_db.get_one(
            "vnfrs", {"nsr-id-ref": nsr_id, "member-vnf-index-ref": member_index}
        )
        return vnfr

    def get_vnfrs(self, nsr_id: str = None, vim_account_id: str = None):
        if nsr_id and vim_account_id:
            raise NotImplementedError("Only one filter is currently supported")
        if nsr_id:
            vnfrs = [
                self.get_vnfr(nsr_id, member["member-vnf-index"])
                for member in self.get_nsr(nsr_id)["nsd"]["constituent-vnfd"]
            ]
        elif vim_account_id:
            vnfrs = self.common_db.get_list("vnfrs", {"vim-account-id": vim_account_id})
        else:
            vnfrs = self.common_db.get_list("vnfrs")
        return vnfrs

    def get_nsr(self, nsr_id: str):
        nsr = self.common_db.get_one("nsrs", {"id": nsr_id})
        return nsr

    def get_vnfds(self):
        return self.common_db.get_list("vnfds")

    def get_monitoring_vnfds(self):
        return self.common_db.get_list(
            "vnfds", {"vdu.monitoring-parameter": {"$exists": "true"}}
        )

    def decrypt_vim_password(self, vim_password: str, schema_version: str, vim_id: str):
        return self.common_db.decrypt(vim_password, schema_version, vim_id)

    def decrypt_sdnc_password(
        self, sdnc_password: str, schema_version: str, sdnc_id: str
    ):
        return self.common_db.decrypt(sdnc_password, schema_version, sdnc_id)

    def get_vim_accounts(self):
        return self.common_db.get_list("vim_accounts")

    def get_vim_account(self, vim_account_id: str) -> dict:
        vim_account = self.common_db.get_one("vim_accounts", {"_id": vim_account_id})
        vim_account["vim_password"] = self.decrypt_vim_password(
            vim_account["vim_password"], vim_account["schema_version"], vim_account_id
        )
        vim_config_encrypted_dict = {
            "1.1": ("admin_password", "nsx_password", "vcenter_password"),
            "default": (
                "admin_password",
                "nsx_password",
                "vcenter_password",
                "vrops_password",
            ),
        }
        vim_config_encrypted = vim_config_encrypted_dict["default"]
        if vim_account["schema_version"] in vim_config_encrypted_dict.keys():
            vim_config_encrypted = vim_config_encrypted_dict[
                vim_account["schema_version"]
            ]
        if "config" in vim_account:
            for key in vim_account["config"]:
                if key in vim_config_encrypted:
                    vim_account["config"][key] = self.decrypt_vim_password(
                        vim_account["config"][key],
                        vim_account["schema_version"],
                        vim_account_id,
                    )
        return vim_account

    def get_sdnc_accounts(self):
        return self.common_db.get_list("sdns")

    def get_sdnc_account(self, sdnc_account_id: str) -> dict:
        sdnc_account = self.common_db.get_one("sdns", {"_id": sdnc_account_id})
        sdnc_account["password"] = self.decrypt_vim_password(
            sdnc_account["password"], sdnc_account["schema_version"], sdnc_account_id
        )
        return sdnc_account

    def get_alert(
        self,
        nsr_id: str,
        vnf_member_index: str,
        vdu_id: str,
        vdu_name: str,
        action_type: str,
    ):
        q_filter = {"action_type": action_type}
        if nsr_id:
            q_filter["tags.ns_id"] = nsr_id
        if vnf_member_index:
            q_filter["tags.vnf_member_index"] = vnf_member_index
        if vdu_id:
            q_filter["tags.vdu_id"] = vdu_id
        if vdu_name:
            q_filter["tags.vdu_name"] = vdu_name
        alert = self.common_db.get_one(
            table="alerts", q_filter=q_filter, fail_on_empty=False
        )
        return alert

    def update_alert_status(self, uuid: str, alarm_status: str):
        modified_count = self.common_db.set_one(
            "alerts", {"uuid": uuid}, {"alarm_status": alarm_status}
        )
        return modified_count

    def create_nslcmop(self, nslcmop: dict):
        self.common_db.create("nslcmops", nslcmop)

    def get_nslcmop(self, nsr_id: str, operation_type: str, since: str):
        q_filter = {}
        if nsr_id:
            q_filter["nsInstanceId"] = nsr_id
        if operation_type:
            q_filter["lcmOperationType"] = operation_type
        if since:
            q_filter["startTime"] = {"$gt": since}
        ops = self.common_db.get_list(table="nslcmops", q_filter=q_filter)
        return ops
