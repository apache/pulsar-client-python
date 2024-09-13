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

import asyncio
from typing import Any
import pulsar
from concurrent.futures import ThreadPoolExecutor
from functools import partial

class AsyncProducer:
    def __init__(self, sync_producer: pulsar.Producer) -> None:
        self.sync_producer = sync_producer

        self._executor = ThreadPoolExecutor(10)
        self._loop = asyncio.get_event_loop()

    async def close(self) -> None:
        return await self._loop.run_in_executor(self._executor, self.sync_producer.close)

    async def flush(self) -> None:
        return await self._loop.run_in_executor(self._executor, self.sync_producer.flush)
    
    async def is_connected(self) -> bool:
        return await self._loop.run_in_executor(self._executor, self.sync_producer.is_connected)
    
    async def last_sequence_id(self) -> int:
        return await self._loop.run_in_executor(self._executor, self.sync_producer.last_sequence_id)
    
    async def producer_name(self) -> str:
        return await self._loop.run_in_executor(self._executor, self.sync_producer.producer_name)
    
    async def send(self, *args, **kwargs):
        sync_method = partial(self.sync_producer.send, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)
    
    async def topic(self) -> str:
        return await self._loop.run_in_executor(self._executor, self.sync_producer.topic)
    
class AsyncReader:
    def __init__(self, sync_reader: pulsar.Reader) -> None:
        self.sync_reader = sync_reader

        self._executor = ThreadPoolExecutor(10)
        self._loop = asyncio.get_event_loop()

    async def close(self):
        return await self._loop.run_in_executor(self._executor, self.sync_reader.close)
    
    async def has_messages_available(self) -> bool:
        return await self._loop.run_in_executor(self._executor, self.sync_reader.has_message_available)
    
    async def is_connected(self) -> bool:
        return await self._loop.run_in_executor(self._executor, self.sync_reader.is_connected)
    
    async def read_next(self, *args, **kwargs):
        sync_method = partial(self.sync_reader.read_next, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)
    
    async def seek(self, *args, **kwargs):
        sync_method = partial(self.sync_reader.seek, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)
    
    async def topic(self) -> str:
        return await self._loop.run_in_executor(self._executor, self.sync_reader.topic)
    
class AsyncConsumer:
    def __init__(self, sync_consumer: pulsar.Consumer):
        self.sync_consumer = sync_consumer

        self._executor = ThreadPoolExecutor(10)
        self._loop = asyncio.get_event_loop()
    
    async def acknowledge(self, *args, **kwargs):
        sync_method = partial(self.sync_consumer.acknowledge, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)

    async def acknowledge_cumulative(self, *args, **kwargs):
        sync_method = partial(self.sync_consumer.acknowledge_cumulative, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)

    async def batch_receive(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.batch_receive)

    async def close(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.close)

    async def consumer_name(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.consumer_name)

    async def get_last_message_id(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.get_last_message_id)

    async def is_connected(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.is_connected)

    async def negative_acknowledge(self, *args, **kwargs):
        sync_method = partial(self.sync_consumer.negative_acknowledge, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)

    async def pause_message_listener(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.pause_message_listener)

    async def receive(self, *args, **kwargs):
        sync_method = partial(self.sync_consumer.receive, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)

    async def redeliver_unacknowledged_messages(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.redeliver_unacknowledged_messages)

    async def resume_message_listener(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.resume_message_listener)

    async def seek(self, *args, **kwargs):
        sync_method = partial(self.sync_consumer.seek, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)

    async def subscription_name(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.subscription_name)

    async def topic(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.topic)

    async def unsubscribe(self):
        return await self._loop.run_in_executor(self._executor, self.sync_consumer.unsubscribe)


class AsyncPulsarClient:
    def __init__(self, *args, **kwargs):
        self.sync_client = pulsar.Client(*args, **kwargs)

        self._executor = ThreadPoolExecutor(10)
        self._loop = asyncio.get_event_loop()

    async def close(self):
        return await self._loop.run_in_executor(self._executor, self.sync_client.close)
    
    async def create_producer(self, *args, **kwargs) -> AsyncProducer:
        sync_create = partial(self.sync_client.create_producer, *args, **kwargs)
        sync_producer = await self._loop.run_in_executor(self._executor, sync_create)
        return AsyncProducer(sync_producer)
    
    async def create_reader(self, *args, **kwargs):
        sync_create = partial(self.sync_client.create_reader, *args, **kwargs)
        sync_reader = await self._loop.run_in_executor(self._executor, sync_create)
        return AsyncReader(sync_reader)
    
    async def get_topic_partitions(self, *args, **kwargs):
        sync_method = partial(self.sync_client.get_topic_partitions, *args, *+kwargs)
        return await self._loop.run_in_executor(self._executor, sync_method)
    
    async def shutdown(self):
        return await self._loop.run_in_executor(self._executor, self.sync_client.shutdown)
    
    async def subscribe(self, *args, **kwargs):
        sync_subscribe = partial(self.sync_client.subscribe, *args, **kwargs)
        sync_consumer = await self._loop.run_in_executor(self._executor, sync_subscribe)
        return AsyncConsumer(sync_consumer)
