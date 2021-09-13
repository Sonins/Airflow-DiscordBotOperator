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

import json
import warnings

from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException, AirflowNotFoundException

from airflow.providers.http.hooks.http import HttpHook

class discordBotWebhookHook(HttpHook):
    """
    This hook allows you to make bot send a message to channel selected.
    Takes a Discord connection ID 
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

    conn_name_attr = "http_conn_id"
    default_conn_name = "discord_default"
    conn_type = "discord"
    hook_name = "Discord_bot"

    def __init__(
        self,
        http_conn_id: Optional[str] = None,
        message: str = "",
        channel: str = "",
        tts: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.token = self._get_token(http_conn_id)
        self.endpoint = self._get_endpoint(http_conn_id, channel)
        self.message = message
        self.tts = tts

    def _get_token(self, http_conn_id: str) -> str:

        conn = self.get_connection(http_conn_id)
        if getattr(conn, 'password', None):
            return conn.password
        elif http_conn_id:
            extra = conn.extra_dejson
            bot_token = extra.get('bot_token', '')
            
            if bot_token:
                warnings.warn(
                    "'password' field is more recommended than 'bot_token' in 'extra'.",
                    Warning,
                    stacklevel=2,
                )

            return bot_token
        else:
            raise AirflowNotFoundException('Cannot get token: No valid token nor http_conn_id supplied.')
    
    def _get_endpoint(self, http_conn_id: str, channel: str) -> str:

        conn = self.get_connection(http_conn_id)

        if channel:
            return f'channels/{channel}/messages'
        
        if http_conn_id:
            extra = conn.extra_dejson
            channel = extra.get('channel', '')
            if channel:
                return f'channels/{channel}/messages'
            
            endpoint = extra.get('endpoint', '')
            if endpoint:
                return endpoint

        else:
            raise AirflowNotFoundException('Cannot get token: No valid channel_id nor http_conn_id supplied.')

    def _build_payload(self, message: str) -> str:

        payload = {}
        if len(message) <= 2000:
            payload['content'] = message
        else:
            raise AirflowException('Discord message length must be 2000 or fewer characters.')
        payload['tts'] = self.tts

        return json.dumps(payload)
        

    def execute(self) -> None:

        discord_payload = self._build_payload(self.message)

        self.run(
            endpoint=self.endpoint,
            data=discord_payload,
            headers={
                'Content-type': 'application/json',
                'Authorization': 'Bot ' + self.token
            },
        )