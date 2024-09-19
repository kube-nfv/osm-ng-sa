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
from typing import Callable, List

from osm_common import msgkafka, msglocal
from osm_ngsa.osm_mon.core.config import Config


class MessageBusClient:
    def __init__(self, config: Config):
        if config.get("message", "driver") == "local":
            self.msg_bus = msglocal.MsgLocal()
        elif config.get("message", "driver") == "kafka":
            self.msg_bus = msgkafka.MsgKafka()
        else:
            raise Exception(
                "Unknown message bug driver {}".format(config.get("section", "driver"))
            )
        self.msg_bus.connect(config.get("message"))

    async def aioread(self, topics: List[str], callback: Callable = None, **kwargs):
        """
        Retrieves messages continuously from bus and executes callback for each message consumed.
        :param topics: List of message bus topics to consume from.
        :param callback: Async callback function to be called for each message received.
        :param kwargs: Keyword arguments to be passed to callback function.
        :return: None
        """
        await self.msg_bus.aioread(topics, aiocallback=callback, **kwargs)

    async def aiowrite(self, topic: str, key: str, msg: dict):
        """
        Writes message to bus.
        :param topic: Topic to write to.
        :param key: Key to write to.
        :param msg: Dictionary containing message to be written.
        :return: None
        """
        await self.msg_bus.aiowrite(topic, key, msg)

    async def aioread_once(self, topic: str):
        """
        Retrieves last message from bus.
        :param topic: topic to retrieve message from.
        :return: tuple(topic, key, message)
        """
        result = await self.msg_bus.aioread(topic)
        return result
