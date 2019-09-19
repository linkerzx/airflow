# -*- coding: utf-8 -*-
#
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
#

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.servicebus import ServiceBusService, Message

from typing import List, Any, TypeVar

log = LoggingMixin().log

AzSBService = TypeVar('azure.servicebus.ServiceBusService')
AzSBMessage = TypeVar('azure.servicebus.Message')

class AzureServiceBusHook(BaseHook):
    """
    Interacts with Azure Service bus.

    :param azure_service_bus_conn_id: Reference to the service bus connection.
    :type azure_service_bus_conn_id: str
    """

    def __init__(self, azure_service_bus_conn_id='wasb_default'):
        self.conn_id = azure_service_bus_conn_id
        self.connection = self.get_conn()

    def get_conn(self) -> AzSBService:
        """Return the Service Bus Service object."""
        conn = self.get_connection(self.conn_id)
        service_options = conn.extra_dejson
        return ServiceBusService(
            conn.host, #Service bus namespace
            shared_access_key_name=conn.login, 
            shared_access_key_value=conn.password
        )

    @staticmethod
    def sb_batch_callback(
            messages_peek: List[AzSBMessage],
            **kwargs
        ) -> Any:
        """
        Process a batch of Azure messages using the preprocessor and processor callback functions. Upon successful response from the callback function, proceed to delete the messages from the service bus subscription and returns the preprocessed data.

        :param  messages_peek: A batch of messages to process
        :type messages_peek: List[AzSBMessage]
        :param callback_preprocessor:
        :type callback_preprocessor: Callable
        :param callback_processor:
        :type callback_processor: Callable
        """
        try:
            callback_pre_processor = kwargs.get("callback_preprocessor")
            callback_processor = kwargs.get("callback_processor")
           
            pre_processed = callback_pre_processor(messages_peek, **kwargs)
            res = callback_processor(pre_processed, **kwargs)
            
            log.debug("Response code %s" % res.status_code)
            if(res.status_code in {200, 201, 202}):
                for message in messages_peek:
                    log.info("Deleting message")
                    try:
                        message.delete()
                    except:
                        log.warning("Couldn't delete message")
            return pre_processed

        except:
            log.error("Error in the Callback")
        return None

    def consume_batch_from_sb_topic(
            self,
            max_batch_size: int,
            sb_topic: str,
            sb_subscription: str,
            **kwargs
        ) -> Any:
        """
        Consume messages from an Azure Service Bus topic subscription and execute a callback

        :param max_batch_size: The maximum number of message to be read in one batch
        :type max_batch_size: int
        :param sb_topic: The Service Bus Topic name
        :type sb_topic: str
        :param sb_subscription: The name of the servie bus subscription
        :type sb_subscription: str 
        :param callback_preprocessor:
        :type callback_preprocessor: Callable 
        :param callback_processor:
        :type callback_processor: Callable
        """
        messages_peek = []
        for i in range(0, max_batch_size):
            message_peek = self.connection.receive_subscription_message(
                sb_topic, sb_subscription, peek_lock=True
            )
            if message_peek.body is not None:
                log.debug(message_peek.body)
                messages_peek.append(message_peek)
        callback_res = self.sb_batch_callback(messages_peek, **kwargs)
        return callback_res
