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

# NOTE: requires version 3.9.0+ of pulsar-client library
import asyncio
from pulsar.asyncio import Client, Producer, Consumer
from pulsar import BatchingType

NUM_MESSAGES = 100
TOPIC_NAME = 'my-async-topic'
SUBSCRIPTION_NAME = 'my-async-subscription'
SERVICE_URL = 'pulsar://localhost:6650'

async def produce(producer: Producer, id: int) -> None:
    await producer.send((f'hello-{id}').encode('utf-8'), None)
    await producer.flush()

async def consume(consumer: Consumer) -> None:
    msg = await consumer.receive()
    print("Received message '{0}' id='{1}'".format(msg.data().decode('utf-8'), msg.message_id()))
    await consumer.acknowledge(msg)

async def main() -> None:
    client: Client = Client(SERVICE_URL)
    consumer = await client.subscribe(TOPIC_NAME, SUBSCRIPTION_NAME,
        properties={
            "consumer-name": "test-consumer-name",
            "consumer-id": "test-consumer-id"
        })

    producer = await client.create_producer(
        TOPIC_NAME,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=10,
        properties={
            "producer-name": "test-producer-name",
            "producer-id": "test-producer-id"
        },
        batching_type=BatchingType.KeyBased
    )

    tasks = []
    for id in range(NUM_MESSAGES):
        tasks.append(asyncio.create_task(produce(producer, id)))
        tasks.append(asyncio.create_task(consume(consumer)))
    await asyncio.gather(*tasks)

    await producer.close()
    await consumer.close()
    await client.close()

if __name__ == '__main__':
    asyncio.run(main())
