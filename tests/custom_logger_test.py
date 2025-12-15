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
import asyncio
import logging
import threading
from pulsar import Client

class CustomLoggingTest(TestCase):

    serviceUrl = 'pulsar://localhost:6650'

    def test_async_func_with_custom_logger(self):
        # boost::python::call may fail in C++ destructors, even worse, calls
        # to PyErr_Print could corrupt the Python interpreter.
        # See https://github.com/boostorg/python/issues/374 for details.
        # This test is to verify these functions won't be called in C++ destructors
        # so that Python's async function works well.
        client = Client(
            self.serviceUrl,
            logger=logging.getLogger('custom-logger')
        )

        async def async_get(value):
            consumer = client.subscribe('test_async_get', 'sub')
            consumer.close()
            return value

        value = 'foo'
        result = asyncio.run(async_get(value))
        self.assertEqual(value, result)

        client.close()

    def test_logger_thread_leaks(self):
        def _do_connect(close):
            logger = logging.getLogger(str(threading.current_thread().ident))
            logger.setLevel(logging.INFO)
            client = Client(
                service_url="pulsar://localhost:6650",
                io_threads=4,
                message_listener_threads=4,
                operation_timeout_seconds=1,
                log_conf_file_path=None,
                authentication=None,
                logger=logger,
            )
            client.get_topic_partitions("persistent://public/default/partitioned_topic_name_test")
            if close:
                client.close()

        for should_close in (True, False):
            self.assertEqual(threading.active_count(), 1, "Explicit close: {}; baseline is 1 thread".format(should_close))
            _do_connect(should_close)
            self.assertEqual(threading.active_count(), 1, "Explicit close: {}; synchronous connect doesn't leak threads".format(should_close))
            threads = []
            for _ in range(10):
                threads.append(threading.Thread(target=_do_connect, args=(should_close)))
                threads[-1].start()
            for thread in threads:
                thread.join()
            assert threading.active_count() == 1, "Explicit close: {}; threaded connect in parallel doesn't leak threads".format(should_close)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
