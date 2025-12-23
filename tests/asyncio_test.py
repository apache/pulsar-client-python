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

"""
Unit tests for asyncio Pulsar client API.
"""

# pylint: disable=missing-function-docstring

import asyncio
import time
from typing import List
from unittest import (
    main,
    IsolatedAsyncioTestCase,
)

import pulsar  # pylint: disable=import-error
from pulsar.asyncio import (  # pylint: disable=import-error
    Client,
    Consumer,
    Producer,
    PulsarException,
)

SERVICE_URL = 'pulsar://localhost:6650'

class AsyncioTest(IsolatedAsyncioTestCase):
    """Test cases for asyncio Pulsar client."""

    async def asyncSetUp(self) -> None:
        self._client = Client(SERVICE_URL,
                              operation_timeout_seconds=5)

    async def asyncTearDown(self) -> None:
        await self._client.close()

    async def test_batch_end_to_end(self):
        topic = f'asyncio-test-batch-e2e-{time.time()}'
        producer = await self._client.create_producer(topic)
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
            self.assertEqual(msg_ids[i].entry_id(), entry_id)
            self.assertEqual(msg_ids[i].batch_index(), i)

        consumer = await self._client.subscribe(topic, 'sub',
                                                initial_position=pulsar.InitialPosition.Earliest)
        for i in range(5):
            msg = await consumer.receive()
            self.assertEqual(msg.data(), f'msg-{i}'.encode())
        await consumer.close()

        # create a different subscription to verify initial position is latest by default
        consumer = await self._client.subscribe(topic, 'sub2')
        await producer.send(b'final-message')
        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'final-message')

    async def test_create_producer_failure(self):
        try:
            await self._client.create_producer('tenant/ns/asyncio-test-send-failure')
            self.fail()
        except PulsarException as e:
            self.assertEqual(e.error(), pulsar.Result.Timeout)

    async def test_send_failure(self):
        producer = await self._client.create_producer('asyncio-test-send-failure')
        try:
            await producer.send(('x' * 1024 * 1024 * 10).encode())
            self.fail()
        except PulsarException as e:
            self.assertEqual(e.error(), pulsar.Result.MessageTooBig)

    async def test_close_producer(self):
        producer = await self._client.create_producer('asyncio-test-close-producer')
        await producer.close()
        try:
            await producer.close()
            self.fail()
        except PulsarException as e:
            self.assertEqual(e.error(), pulsar.Result.AlreadyClosed)

    async def _prepare_messages(self, producer: Producer) -> List[pulsar.MessageId]:
        msg_ids = []
        for i in range(5):
            msg_ids.append(await producer.send(f'msg-{i}'.encode()))
        return msg_ids

    async def test_consumer_cumulative_acknowledge(self):
        topic = f'asyncio-test-consumer-cumulative-ack-{time.time()}'
        sub = 'sub'
        consumer = await self._client.subscribe(topic, sub)
        producer = await self._client.create_producer(topic)
        await self._prepare_messages(producer)
        last_msg = None
        for _ in range(5):
            last_msg = await consumer.receive()
        await consumer.acknowledge_cumulative(last_msg)
        await consumer.close()

        consumer = await self._client.subscribe(topic, sub)
        await producer.send(b'final-message')
        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'final-message')

    async def test_consumer_individual_acknowledge(self):
        topic = f'asyncio-test-consumer-individual-ack-{time.time()}'
        sub = 'sub'
        consumer = await self._client.subscribe(topic, sub,
                                                consumer_type=pulsar.ConsumerType.Shared)
        producer = await self._client.create_producer(topic)
        await self._prepare_messages(producer)
        msgs = []
        for _ in range(5):
            msg = await consumer.receive()
            msgs.append(msg)

        await consumer.acknowledge(msgs[0])
        await consumer.acknowledge(msgs[2])
        await consumer.acknowledge(msgs[4])
        await consumer.close()

        consumer = await self._client.subscribe(topic, sub,
                                                consumer_type=pulsar.ConsumerType.Shared)
        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'msg-1')
        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'msg-3')

    async def test_multi_topic_consumer(self):
        topics = ['asyncio-test-multi-topic-1', 'asyncio-test-multi-topic-2']
        producers = []

        for topic in topics:
            producer = await self._client.create_producer(topic)
            producers.append(producer)

        consumer = await self._client.subscribe(topics, 'test-multi-subscription')

        await producers[0].send(b'message-from-topic-1')
        await producers[1].send(b'message-from-topic-2')

        async def verify_receive(consumer: Consumer):
            received_messages = {}
            for _ in range(2):
                msg = await consumer.receive()
                received_messages[msg.data()] = None
                await consumer.acknowledge(msg.message_id())
            self.assertEqual(received_messages, {
                b'message-from-topic-1': None,
                b'message-from-topic-2': None
            })

        await verify_receive(consumer)
        await consumer.close()

        consumer = await self._client.subscribe('public/default/asyncio-test-multi-topic-.*',
                                                'test-multi-subscription-2',
                                                is_pattern_topic=True,
                                                initial_position=pulsar.InitialPosition.Earliest)
        await verify_receive(consumer)
        await consumer.close()

    async def test_unsubscribe(self):
        topic = f'asyncio-test-unsubscribe-{time.time()}'
        sub = 'sub'
        consumer = await self._client.subscribe(topic, sub)
        await consumer.unsubscribe()
        # Verify the consumer can be created successfully with the same subscription name
        consumer = await self._client.subscribe(topic, sub)
        await consumer.close()

    async def test_seek_message_id(self):
        topic = f'asyncio-test-seek-message-id-{time.time()}'
        sub = 'sub'

        producer = await self._client.create_producer(topic)
        msg_ids = await self._prepare_messages(producer)

        consumer = await self._client.subscribe(
            topic, sub, initial_position=pulsar.InitialPosition.Earliest
        )
        await consumer.seek(msg_ids[2])
        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'msg-3')
        await consumer.close()

        consumer = await self._client.subscribe(
            topic, sub, initial_position=pulsar.InitialPosition.Earliest,
            start_message_id_inclusive=True
        )
        await consumer.seek(msg_ids[2])
        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'msg-2')
        await consumer.close()

    async def test_seek_timestamp(self):
        topic = f'asyncio-test-seek-timestamp-{time.time()}'
        sub = 'sub'
        consumer = await self._client.subscribe(
            topic, sub, initial_position=pulsar.InitialPosition.Earliest
        )

        producer = await self._client.create_producer(topic)

        # Send first 3 messages
        for i in range(3):
            await producer.send(f'msg-{i}'.encode())

        seek_time = int(time.time() * 1000)

        # Send 2 more messages
        for i in range(3, 5):
            await producer.send(f'msg-{i}'.encode())

        # Consume all messages first
        for i in range(5):
            msg = await consumer.receive()
            self.assertEqual(msg.data(), f'msg-{i}'.encode())

        # Seek to the timestamp (should start from msg-3)
        await consumer.seek(seek_time)

        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'msg-3')


if __name__ == '__main__':
    main()
