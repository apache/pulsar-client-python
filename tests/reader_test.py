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

from unittest import TestCase, main
import time

from pulsar import Client, MessageId

class ReaderTest(TestCase):

    def setUp(self):
        self._client: Client = Client('pulsar://localhost:6650')

    def tearDown(self) -> None:
        self._client.close()

    def test_has_message_available_after_seek(self):
        topic = f'test_has_message_available_after_seek-{time.time()}'
        producer = self._client.create_producer(topic)
        reader = self._client.create_reader(topic, start_message_id=MessageId.earliest)

        producer.send('msg-0'.encode())
        self.assertTrue(reader.has_message_available())

        reader.seek(MessageId.latest)
        self.assertFalse(reader.has_message_available())

        producer.send('msg-1'.encode())
        self.assertTrue(reader.has_message_available())

    def test_seek_latest_message_id(self):
        topic = f'test_seek_latest_message_id-{time.time()}'
        producer = self._client.create_producer(topic)
        msg_id = producer.send('msg'.encode())

        reader = self._client.create_reader(topic,
            start_message_id=MessageId.latest)
        self.assertFalse(reader.has_message_available())
        reader.close()

        reader = self._client.create_reader(topic,
            start_message_id=MessageId.latest,
            start_message_id_inclusive=True)
        self.assertTrue(reader.has_message_available())
        msg = reader.read_next(3000)
        self.assertEqual(msg.message_id(), msg_id)
        reader.close()


if __name__ == "__main__":
    main()
