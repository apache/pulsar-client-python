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

from _pulsar import ConsumerType

import pulsar
from pulsar.asyncio import (
    Client,
    PulsarException,
)
from unittest import (
    main,
    IsolatedAsyncioTestCase,
)

# TODO: Write tests for everything else

service_url = 'pulsar://159.69.189.225'

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
            self.assertEqual(msg_ids[i].entry_id(), entry_id)
            self.assertEqual(msg_ids[i].batch_index(), i)

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

if __name__ == '__main__':
    main()
