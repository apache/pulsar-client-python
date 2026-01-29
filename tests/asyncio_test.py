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
import _pulsar # pylint: disable=import-error
from pulsar.asyncio import (  # pylint: disable=import-error
    Client,
    Consumer,
    Producer,
    PulsarException,
)
from pulsar.schema import (  # pylint: disable=import-error
    AvroSchema,
    Integer,
    Record,
    String,
)

from urllib.request import urlopen, Request

SERVICE_URL = 'pulsar://localhost:6650'
ADMIN_URL = "http://localhost:8080"
TIMEOUT_MS = 10000  # Do not wait forever in tests

def doHttpPost(url, data):
    req = Request(url, data.encode())
    req.add_header("Content-Type", "application/json")
    urlopen(req)

def doHttpPut(url, data):
    try:
        req = Request(url, data.encode())
        req.add_header("Content-Type", "application/json")
        req.get_method = lambda: "PUT"
        urlopen(req)
    except Exception as ex:
        # ignore conflicts exception to have test idempotency
        if "409" in str(ex):
            pass
        else:
            raise ex

class AsyncioTest(IsolatedAsyncioTestCase):
    """Test cases for asyncio Pulsar client."""

    async def asyncSetUp(self) -> None:
        self._client = Client(SERVICE_URL,
                              operation_timeout_seconds=5)

    async def asyncTearDown(self) -> None:
        await self._client.close()

    async def test_batch_end_to_end(self):
        topic = f'asyncio-test-batch-e2e-{time.time()}'
        producer = await self._client.create_producer(topic,
                                                      producer_name="my-producer")
        self.assertEqual(producer.topic(), f'persistent://public/default/{topic}')
        self.assertEqual(producer.producer_name(), "my-producer")
        tasks = []
        for i in range(5):
            tasks.append(asyncio.create_task(producer.send(f'msg-{i}'.encode())))
        msg_ids = await asyncio.gather(*tasks)
        self.assertEqual(len(msg_ids), 5)
        # pylint: disable=fixme
        # TODO: the result is wrong due to https://github.com/apache/pulsar-client-cpp/issues/531
        self.assertEqual(producer.last_sequence_id(), 8)
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

    async def test_send_keyed_message(self):
        topic = f'asyncio-test-send-keyed-message-{time.time()}'
        producer = await self._client.create_producer(topic)
        consumer = await self._client.subscribe(topic, 'sub')
        await producer.send(b'msg', partition_key='key0',
                                     ordering_key="key1", properties={'my-prop': 'my-value'})

        msg = await consumer.receive()
        self.assertEqual(msg.data(), b'msg')
        self.assertEqual(msg.partition_key(), 'key0')
        self.assertEqual(msg.ordering_key(), 'key1')
        self.assertEqual(msg.properties(), {'my-prop': 'my-value'})

    async def test_flush(self):
        topic = f'asyncio-test-flush-{time.time()}'
        producer = await self._client.create_producer(topic, batching_max_messages=3,
                                                      batching_max_publish_delay_ms=60000)
        tasks = []
        tasks.append(asyncio.create_task(producer.send(b'msg-0')))
        tasks.append(asyncio.create_task(producer.send(b'msg-1')))

        done, pending = await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_COMPLETED)
        self.assertEqual(len(done), 0)
        self.assertEqual(len(pending), 2)

        # flush will trigger sending the batched messages
        await producer.flush()
        for task in pending:
            self.assertTrue(task.done())
        msg_id0 = tasks[0].result()
        msg_id1 = tasks[1].result()
        self.assertEqual(msg_id0.ledger_id(), msg_id1.ledger_id())
        self.assertEqual(msg_id0.entry_id(), msg_id1.entry_id())
        self.assertEqual(msg_id0.batch_index(), 0)
        self.assertEqual(msg_id1.batch_index(), 1)

    async def test_get_topics_partitions(self):
        topic_partitioned = "persistent://public/default/test_get_topics_partitions_async"
        topic_non_partitioned = "persistent://public/default/test_get_topics_async_not-partitioned"

        url1 = ADMIN_URL + "/admin/v2/persistent/public/default/test_get_topics_partitions_async/partitions"
        doHttpPut(url1, "3")

        self.assertEqual(
            await self._client.get_topic_partitions(topic_partitioned),
            [
                "persistent://public/default/test_get_topics_partitions_async-partition-0",
                "persistent://public/default/test_get_topics_partitions_async-partition-1",
                "persistent://public/default/test_get_topics_partitions_async-partition-2",
            ],
        )
        self.assertEqual(await self._client.get_topic_partitions(topic_non_partitioned), [topic_non_partitioned])

    async def test_get_partitioned_topic_name(self):
        url1 = ADMIN_URL + "/admin/v2/persistent/public/default/partitioned_topic_name_test/partitions"
        doHttpPut(url1, "3")

        partitions = [
            "persistent://public/default/partitioned_topic_name_test-partition-0",
            "persistent://public/default/partitioned_topic_name_test-partition-1",
            "persistent://public/default/partitioned_topic_name_test-partition-2",
        ]
        self.assertEqual(
            await self._client.get_topic_partitions("persistent://public/default/partitioned_topic_name_test"), partitions
        )

        consumer = await self._client.subscribe(
            "persistent://public/default/partitioned_topic_name_test",
            "partitioned_topic_name_test_sub",
            consumer_type=pulsar.ConsumerType.Shared,
        )
        producer = await self._client.create_producer("persistent://public/default/partitioned_topic_name_test")
        await producer.send(b"hello")

        async with asyncio.timeout(TIMEOUT_MS / 1000):
            msg = await consumer.receive()
        self.assertTrue(msg.topic_name() in partitions)

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

    async def test_producer_is_connected(self):
        topic = f'asyncio-test-producer-is-connected-{time.time()}'
        producer = await self._client.create_producer(topic)
        self.assertTrue(producer.is_connected())
        await producer.close()
        self.assertFalse(producer.is_connected())

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

    async def test_consumer_negative_acknowledge(self):
        topic = f'asyncio-test-consumer-negative-ack-{time.time()}'
        sub = 'sub'
        consumer = await self._client.subscribe(topic, sub,
                                                consumer_type=pulsar.ConsumerType.Shared,
                                                negative_ack_redelivery_delay_ms=100)
        
        producer = await self._client.create_producer(topic)
        await self._prepare_messages(producer)
        msgs = []
        for _ in range(5):
            msg = await consumer.receive()
            msgs.append(msg)

        await consumer.acknowledge(msgs[1])
        await consumer.acknowledge(msgs[3])
        
        await consumer.negative_acknowledge(msgs[0])
        await consumer.negative_acknowledge(msgs[2])
        await consumer.negative_acknowledge(msgs[4])
        await asyncio.sleep(0.2)
        
        received = []
        for _ in range(3):
            msg = await consumer.receive()
            received.append(msg.data())
        
        self.assertEqual(sorted(received), [b'msg-0', b'msg-2', b'msg-4'])
        await consumer.close()

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

    async def test_consumer_get_last_message_id(self):
        topic = f'asyncio-test-get-last-message-id-{time.time()}'
        sub = 'sub'
        consumer = await self._client.subscribe(topic, sub,
                                                consumer_type=pulsar.ConsumerType.Shared)
        producer = await self._client.create_producer(topic)
        for i in range(5):
            msg = f'msg-{i}'.encode()
            await producer.send(msg)
            last_msg_id = await consumer.get_last_message_id()
            assert isinstance(last_msg_id, _pulsar.MessageId)
            assert last_msg_id.entry_id() == i
            await consumer.acknowledge(last_msg_id)
        await consumer.close()

    async def test_async_dead_letter_policy(self):
        topic = f'asyncio-test-dlq-{time.time()}'
        dlq_topic = 'dlq-' + topic
        max_redeliver_count = 5

        dlq_consumer = await self._client.subscribe(dlq_topic, "my-sub", consumer_type=pulsar.ConsumerType.Shared)
        consumer = await self._client.subscribe(topic, "my-sub", consumer_type=pulsar.ConsumerType.Shared,
                                    dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(max_redeliver_count, dlq_topic, 'init-sub'))
        producer = await self._client.create_producer(topic)

        # Sen num msgs.
        num = 10
        for i in range(num):
            await producer.send(b"hello-%d" % i)
        await producer.flush()

        # Redelivery all messages maxRedeliverCountNum time.
        for i in range(1, num * max_redeliver_count + num + 1):
            msg = await consumer.receive()
            if i % num == 0:
                consumer.redeliver_unacknowledged_messages()
                print(f"Start redeliver msgs '{i}'")

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(consumer.receive(), 0.1)

        for i in range(num):
            msg = await dlq_consumer.receive()
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)
            dlq_consumer.acknowledge(msg)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(dlq_consumer.receive(), 0.1)
            
        await consumer.close()
        await dlq_consumer.close()

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

    async def test_schema(self):
        class ExampleRecord(Record):  # pylint: disable=too-few-public-methods
            """Example record schema for testing."""
            str_field = String()
            int_field = Integer()

        topic = f'asyncio-test-schema-{time.time()}'
        producer = await self._client.create_producer(
                topic, schema=AvroSchema(ExampleRecord)
        )
        consumer = await self._client.subscribe(
            topic, 'sub', schema=AvroSchema(ExampleRecord)
        )
        await producer.send(ExampleRecord(str_field='test', int_field=42))
        msg = await consumer.receive()
        self.assertIsInstance(msg.value(), ExampleRecord)
        self.assertEqual(msg.value().str_field, 'test')
        self.assertEqual(msg.value().int_field, 42)


if __name__ == '__main__':
    main()
