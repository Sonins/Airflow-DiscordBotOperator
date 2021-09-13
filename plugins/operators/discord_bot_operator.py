# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any, Dict

from airflow.providers.http.operators.http import SimpleHttpOperator
from hooks.discord_bot_hook import DiscordBotWebhookHook


class DiscordBotOperator(SimpleHttpOperator):
    """
    This operator allows you to make bot send a message.
    :param http_conn_id: Http connection ID with host as "https://discord.com/api/" and
                         default webhook endpoint in the extra field in the form of
                         {"endpoint": "channels/{channel_id}/messages",
                          "channel": "CHANNEL_ID"}.
                         If you want supply a channel information with connection
                         extra field, either endpoint or channel should be specified.
    :type http_conn_id: str
    :param message: The message you want to send to your Discord channel
                    (max 2000 characters)
    :type message: str
    :param channel: Channel id where bot should send a message.
    :type channel: str
    :param tts: Is a text-to-speech message
    :type tts: bool
    """

    def __init__(
        self,
        http_conn_id: str,
        message: str = "",
        channel: str = "",
        tts: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.message = message
        self.channel = channel
        self.tts = tts

    def execute(self, context: Dict[str, Any]) -> Any:
        """Call DiscordBotWebhookHook to send messages."""
        self.hook = DiscordBotWebhookHook(
            http_conn_id=self.http_conn_id,
            channel=self.channel,
            message=self.message,
            tts=self.tts,
        )
        self.hook.execute()
