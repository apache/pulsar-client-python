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

from typing import Callable
from unittest import TestCase, main
import time

from pulsar import Client
from pulsar.schema.schema import StringSchema

class TableViewTest(TestCase):

    def setUp(self):
        self._client: Client = Client('pulsar://localhost:6650')

    def tearDown(self):
        self._client.close()

    def test_get(self):
        topic = f'table_view_test_get-{time.time()}'
        table_view = self._client.create_table_view(topic)
        self.assertEqual(len(table_view), 0)

        producer = self._client.create_producer(topic)
        producer.send(b'value-0', partition_key='key-0')
        producer.send(b'\xba\xd0\xba\xd0', partition_key='key-1') # an invalid UTF-8 bytes

        self._wait_for_assertion(lambda: self.assertEqual(len(table_view), 2))
        self.assertEqual(table_view.get('key-0'), b'value-0')
        self.assertEqual(table_view.get('key-1'), b'\xba\xd0\xba\xd0')

        producer.send(b'value-1', partition_key='key-0')
        self._wait_for_assertion(lambda: self.assertEqual(table_view.get('key-0'), b'value-1'))

        producer.close()
        table_view.close()
        
    def test_for_each(self):
        topic = f'table_view_test_for_each-{time.time()}'
        table_view = self._client.create_table_view(topic)
        producer = self._client.create_producer(topic)
        producer.send(b'value-0', partition_key='key-0')
        producer.send(b'value-1', partition_key='key-1')
        self._wait_for_assertion(lambda: self.assertEqual(len(table_view), 2))

        d = dict()
        table_view.for_each(lambda key, value: d.__setitem__(key, value))
        self.assertEqual(d, {
            'key-0': b'value-0',
            'key-1': b'value-1'
        })

        def listener(key: str, value: str):
            if len(value) == 0:
                d.pop(key)
            else:
                d[key] = value

        d.clear()
        table_view.for_each_and_listen(listener)
        self.assertEqual(d, {
            'key-0': b'value-0',
            'key-1': b'value-1'
        })

        producer.send(b'value-0-new', partition_key='key-0')
        producer.send(b'', partition_key='key-1')
        producer.send(b'value-2', partition_key='key-2')
        def assert_latest_values():
            self.assertEqual(d, {
                'key-0': b'value-0-new',
                'key-2': b'value-2'
            })
        self._wait_for_assertion(assert_latest_values)

    def test_schema(self):
        topic = f'table_view_test_schema-{time.time()}'
        table_view = self._client.create_table_view(topic, schema=StringSchema())
        producer = self._client.create_producer(topic, schema=StringSchema())
        producer.send('value', partition_key='key')

        self._wait_for_assertion(lambda: self.assertEqual(table_view.get('key'), 'value'))
        self.assertEqual(table_view.get('missed-key'), None)

        entries = dict()
        table_view.for_each(lambda key, value: entries.__setitem__(key, value))
        self.assertEqual(entries, {'key': 'value'})

        entries.clear()
        table_view.for_each_and_listen(lambda key, value: entries.__setitem__(key, value))
        self.assertEqual(entries, {'key': 'value'})

        producer.send('new-value', partition_key='key')
        self._wait_for_assertion(lambda: self.assertEqual(table_view.get('key'), 'new-value'))

    def _wait_for_assertion(self, assertion: Callable, timeout=5) -> None:
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                assertion()
                return
            except AssertionError:
                time.sleep(0.1)
        assertion()

if __name__ == "__main__":
    main()
