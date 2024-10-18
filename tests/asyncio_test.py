#!/usr/bin/env python3
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
from typing import Iterable

from _pulsar import ConsumerType

import pulsar
from pulsar.asyncio import (
    Client,
    PulsarException,
    Consumer
)
from unittest import (
    main,
    IsolatedAsyncioTestCase,
)

# TODO: Write tests for everything else

service_url = 'pulsar://localhost'

class AsyncioTest(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self._client = Client(service_url)

    async def asyncTearDown(self) -> None:
        await self._client.close()

    async def test_batch_send(self):
        producer = await self._client.create_producer('awaitio-test-batch-send')
        tasks = []
        for i in range(5):
            tasks.append(asyncio.create_task(producer.send(f'msg-{i}'.encode())))
        msg_ids = await asyncio.gather(*tasks)
        self.assertEqual(len(msg_ids), 5)
        ledger_id = msg_ids[0].ledger_id()
        entry_id = msg_ids[0].entry_id()
        # These messages should be in the same entry
        for i in range(5):
            msg_id = msg_ids[i]
            print(f'{i} was sent to {msg_id}')
            self.assertIsInstance(msg_id, pulsar.MessageId)
            self.assertEqual(msg_ids[i].ledger_id(), ledger_id)

    async def test_create_producer_failure(self):
        try:
            await self._client.create_producer('tenant/ns/awaitio-test-send-failure')
            self.fail()
        except PulsarException as e:
            # self.assertEqual(e.error(), pulsar.Result.AuthorizationError or pulsar.Result.TopicNotFound)
            self.assertTrue(e.error() == pulsar.Result.AuthorizationError or e.error() == pulsar.Result.TopicNotFound)

    async def test_send_failure(self):
        producer = await self._client.create_producer('awaitio-test-send-failure')
        try:
            await producer.send(('x' * 1024 * 1024 * 10).encode())
            self.fail()
        except PulsarException as e:
            self.assertEqual(e.error(), pulsar.Result.MessageTooBig)

    async def test_close_producer(self):
        producer = await self._client.create_producer('awaitio-test-close-producer')
        await producer.close()
        try:
            await producer.close()
            self.fail()
        except PulsarException as e:
            self.assertEqual(e.error(), pulsar.Result.AlreadyClosed)

    async def test_subscribe(self):
        consumer = await self._client.subscribe('awaitio-test-close-producer', 'test-subscription')
        self.assertIsInstance(consumer, Consumer)

    async def test_read_and_ack(self):
        test_producer = await self._client.create_producer("awaitio-test-consumer-ack")
        consumer = await self._client.subscribe('awaitio-test-consumer-ack', 'test-subscription')

        await test_producer.send(b"test123")
        msg = await consumer.receive()

        self.assertEqual(msg.data(), b"test123")

        await consumer.acknowledge(msg)

    async def test_batch_read_and_ack(self):
        test_producer = await self._client.create_producer("awaitio-test-consumer-ack-batch")
        consumer = await self._client.subscribe('awaitio-test-consumer-ack-batch', 'test-subscription')

        await test_producer.send(b"test123")
        msgs = await consumer.batch_receive()

        last = None
        for msg in msgs:
            last = msg

        await consumer.acknowledge_cumulative(last)

        self.assertIsInstance(msgs, Iterable)
        for msg in msgs:
            self.assertEqual(b"test123", msg.data())

    async def test_consumer_close(self):
        consumer = await self._client.subscribe('awaitio-test-consumer-close', 'test-subscription')
        await consumer.close()

        self.assertFalse(consumer.is_connected)

    async def test_consumer_seek(self):
        consumer = await self._client.subscribe('awaitio-test-consumer-close', 'test-subscription')
        await consumer.seek(consumer.last_message_id)

    async def test_consumer_unsubscribe(self):
        consumer = await self._client.subscribe('awaitio-test-consumer-close', 'test-subscription')
        await consumer.unsubscribe()

    

if __name__ == '__main__':
    main()
