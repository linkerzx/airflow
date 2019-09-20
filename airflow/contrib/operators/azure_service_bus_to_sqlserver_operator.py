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

from airflow.contrib.hooks.azure_service_bus_hook import AzureServiceBusHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import TyeVar
import json
import pandas as pd

PandasDataFrame = TypeVar('pandas.core.frame.DataFrame')
AzSBMessage = TypeVar('azure.servicebus.Message')

def default_sb_json_messages_to_df_preprocessor(
        messages_peek: List[AzSBMessage].
        **kwargs
    ) -> PandasDataFrame:
    """
    <PreProcessor Callback>
    Converts a list of json message into a dataframe
    :param messages_peek: List of Sb messages to convert to a dataframe
    :type messages_peek: List[AzSBMessage]
    :param preprocessor_convert_to_string: Wether to convert the dataframe to string
    :return: A dataframe object
    """
    if len(messages_peek) > 0:
        df = pd.concat([
            json.loads(sb_msg.body)
            for sb_msg in messages_peek
        ])
        if kwargs.get("preprocessor_convert_to_string"):
            df = df.astype(str)
        return df
    else:
        return pd.DataFrame([])

def df_to_mssql_processor(
        df: PandasDataFrame,
        **kwargs
    ):
    """
    <Processor Callback>
    Sends the DataFrame data to an SQL Server table.
    The processor assume that the data is already in the right format

    :param df:
    :type df: PandasDataFrame
    :param conn_id: The connection id for the sql server credentials as defined in airflow
    :type conn_id: str
    :param db_table_name: The table naem to use for inserting the data 
    :type db_table_name: str
    :param db_chunk_size: the Chunk size to use when inserting data
    :type db_chunk_size: str
    """
    import mock
    db_conn_id = kwargs.get("conn_id")
    table_name = kwargs.get("db_table_name")
    chunk_size = kwargs.get("db_chunk_size")
    res = mock.Mock()
    engine = create_mssql_engine(db_conn_id)
    try:
        with engine.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists='append',
                index=False,
                chunksize=chunk_size
            )
        res.status_code = 200
    except:
       res.status_code = 500 
    return res

class AzureServiceBusToSQLServerOperator(BaseOperator):
    """
    Ingest data from a Service Bus topic Subscription and pushes it to SQL Server.
    It will create the database table if it doensn't already exists.

    By default it expects the message to be pushed in a flat k,v json format, but it is possible to modify the data by providing a different preprocesor.

    :param azure_service_bus_conn_id:
    :param mssql_conn_id:
    :param batch_size:
    :param sb_topic: The service bus topic to connect to
    :type sb_topic: str
    :param sb_subscription:
    :type sb_subscription: str
    :param callback_preprocessor:
    :param callback_processor:
    :param db_table_name: The table naem to use for inserting the data 
    :type db_table_name: str
    :param db_chunk_size: The Chunk size to use when inserting data
    :type db_chunk_size: str
    """

    @apply_defaults
    def __init__(self,
                 azure_service_bus_conn_id='wasb_default',
                 mssql_conn_id="", # add default placeholder,
                 batch_size=100,
                 sb_topic="",
                 sb_subscription="",
                 db_table_name="",
                 db_chunk_size="",
                 callback_preprocessor=default_json_messages_to_df_preprocessor,
                 callback_processor=df_to_mssql_processor,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.azure_service_bus_conn_id = azure_service_bus_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.batch_size = batch_size
        self.sb_topic = sb_topic
        self.sb_subscription = sb_subscription
        self.callback_preprocessor = callback_preprocessor
        self.db_table_name = db_table_name
        self.db_chunk_size = db_chunk_size

    def execute(self, context):
        # Create the Service bus hook
        sb_hook = AzureServiceBusHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)
       
       # while msg in topic
        messages_count = 1
        while messages_count > 0: 
            messages_count, res = sb_hook.consume_batch_from_sb_topic(
                    self.batch_size,
                    self.sb_topic
                    self.sb_subscription,
                    callback_preprocessor=self.callback_preprocessor,
                    callback_processor=self.callback_processor,
                    conn_id=self.mssql_conn_id
            )
