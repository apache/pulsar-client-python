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


import threading
import logging
from unittest import TestCase, main
import time
import os
import pulsar
import uuid
from datetime import timedelta
from pulsar import (
    Client,
    MessageId,
    CompressionType,
    ConsumerType,
    KeySharedMode,
    ConsumerKeySharedPolicy,
    PartitionsRoutingMode,
    AuthenticationBasic,
    AuthenticationTLS,
    Authentication,
    AuthenticationToken,
    InitialPosition,
    CryptoKeyReader,
    ConsumerBatchReceivePolicy,
    ProducerAccessMode,
)
from pulsar.schema import JsonSchema, Record, Integer

from _pulsar import ProducerConfiguration, ConsumerConfiguration

from schema_test import *

from urllib.request import urlopen, Request

TM = 10000  # Do not wait forever in tests
CERTS_DIR = os.path.dirname(os.path.abspath(__file__)) + "/test-conf/"

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


def doHttpGet(url):
    req = Request(url)
    req.add_header("Accept", "application/json")
    return urlopen(req).read()


class TestRecord(Record):
    a = Integer()
    b = Integer()


class PulsarTest(TestCase):

    serviceUrl = "pulsar://localhost:6650"
    adminUrl = "http://localhost:8080"

    serviceUrlTls = "pulsar+ssl://localhost:6651"

    def test_producer_config(self):
        conf = ProducerConfiguration()
        conf.send_timeout_millis(12)
        self.assertEqual(conf.send_timeout_millis(), 12)

        self.assertEqual(conf.compression_type(), CompressionType.NONE)
        conf.compression_type(CompressionType.LZ4)
        self.assertEqual(conf.compression_type(), CompressionType.LZ4)

        conf.max_pending_messages(120)
        self.assertEqual(conf.max_pending_messages(), 120)

    def test_consumer_config(self):
        conf = ConsumerConfiguration()
        self.assertEqual(conf.consumer_type(), ConsumerType.Exclusive)
        conf.consumer_type(ConsumerType.Shared)
        self.assertEqual(conf.consumer_type(), ConsumerType.Shared)

        self.assertEqual(conf.consumer_name(), "")
        conf.consumer_name("my-name")
        self.assertEqual(conf.consumer_name(), "my-name")

        self.assertEqual(conf.replicate_subscription_state_enabled(), False)
        conf.replicate_subscription_state_enabled(True)
        self.assertEqual(conf.replicate_subscription_state_enabled(), True)

    def test_connect_error(self):
        with self.assertRaises(ValueError):
            Client("fakeServiceUrl")

    def test_exception_inheritance(self):
        assert issubclass(pulsar.ConnectError, pulsar.PulsarException)
        assert issubclass(pulsar.PulsarException, Exception)

    def test_simple_producer(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer("my-python-topic")
        producer.send(b"hello")
        producer.close()
        client.close()

    def test_producer_send_async(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer("my-python-topic")

        sent_messages = []

        def send_callback(producer, msg):
            sent_messages.append(msg)

        producer.send_async(b"hello", send_callback)
        producer.send_async(b"hello", send_callback)
        producer.send_async(b"hello", send_callback)

        i = 0
        while len(sent_messages) < 3 and i < 100:
            time.sleep(0.1)
            i += 1
        self.assertEqual(len(sent_messages), 3)
        client.close()

    def test_producer_send(self):
        client = Client(self.serviceUrl)
        topic = "test_producer_send"
        producer = client.create_producer(topic)
        consumer = client.subscribe(topic, "sub-name")
        msg_id = producer.send(b"hello")
        print("send to {}".format(msg_id))
        msg = consumer.receive(TM)
        consumer.acknowledge(msg)
        print("receive from {}".format(msg.message_id()))
        self.assertEqual(msg_id, msg.message_id())
        client.close()

    def test_producer_access_mode_exclusive(self):
        client = Client(self.serviceUrl)
        topic_name = "test-access-mode-exclusive"
        client.create_producer(topic_name, producer_name="p1", access_mode=ProducerAccessMode.Exclusive)
        with self.assertRaises(pulsar.ProducerFenced):
            client.create_producer(topic_name, producer_name="p2", access_mode=ProducerAccessMode.Exclusive)
        client.close()

    def test_producer_access_mode_wait_exclusive(self):
        client = Client(self.serviceUrl)
        topic_name = "test_producer_access_mode_wait_exclusive"
        producer1 = client.create_producer(
            topic=topic_name,
            producer_name='p-1',
            access_mode=ProducerAccessMode.Exclusive
        )
        assert producer1.producer_name() == 'p-1'

        # when p1 close, p2 success created.
        producer1.close()
        producer2 = client.create_producer(
            topic=topic_name,
            producer_name='p-2',
            access_mode=ProducerAccessMode.WaitForExclusive
        )
        assert producer2.producer_name() == 'p-2'

        producer2.close()
        client.close()

    def test_producer_access_mode_exclusive_with_fencing(self):
        client = Client(self.serviceUrl)
        topic_name = 'test_producer_access_mode_exclusive_with_fencing'

        producer1 = client.create_producer(
            topic=topic_name,
            producer_name='p-1',
            access_mode=ProducerAccessMode.Exclusive
        )
        assert producer1.producer_name() == 'p-1'

        producer2 = client.create_producer(
            topic=topic_name,
            producer_name='p-2',
            access_mode=ProducerAccessMode.ExclusiveWithFencing
        )
        assert producer2.producer_name() == 'p-2'

        # producer1 will be fenced.
        with self.assertRaises((pulsar.ProducerFenced, pulsar.AlreadyClosed)):
            producer1.send('test-msg'.encode('utf-8'))
        # sleep 200ms to make sure producer1 is close done.
        time.sleep(0.2)

        producer2.close()
        client.close()

    def test_producer_is_connected(self):
        client = Client(self.serviceUrl)
        topic = "test_producer_is_connected"
        producer = client.create_producer(topic)
        self.assertTrue(producer.is_connected())
        producer.close()
        self.assertFalse(producer.is_connected())
        client.close()

    def test_producer_consumer(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe("my-python-topic-producer-consumer", "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer("my-python-topic-producer-consumer")
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        consumer.unsubscribe()
        client.close()

    def test_redelivery_count(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe(
            "my-python-topic-redelivery-count",
            "my-sub",
            consumer_type=ConsumerType.Shared,
            negative_ack_redelivery_delay_ms=500,
        )
        producer = client.create_producer("my-python-topic-redelivery-count")
        producer.send(b"hello")

        redelivery_count = 0
        for i in range(4):
            msg = consumer.receive(TM)
            print("Received message %s" % msg.data())
            consumer.negative_acknowledge(msg)
            redelivery_count = msg.redelivery_count()

        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        self.assertEqual(3, redelivery_count)
        consumer.unsubscribe()
        producer.close()
        client.close()

    def test_deliver_at(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe("my-python-topic-deliver-at", "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer("my-python-topic-deliver-at")
        # Delay message in 1.1s
        producer.send(b"hello", deliver_at=int(round(time.time() * 1000)) + 1100)

        # Message should not be available in the next second
        with self.assertRaises(pulsar.Timeout):
            consumer.receive(1000)

        # Message should be published now
        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        consumer.unsubscribe()
        producer.close()
        client.close()

    def test_deliver_after(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe("my-python-topic-deliver-after", "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer("my-python-topic-deliver-after")
        # Delay message in 1.1s
        producer.send(b"hello", deliver_after=timedelta(milliseconds=1100))

        # Message should not be available in the next second
        with self.assertRaises(pulsar.Timeout):
            consumer.receive(1000)

        # Message should be published in the next 500ms
        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        consumer.unsubscribe()
        producer.close()
        client.close()

    def test_consumer_initial_position(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer("consumer-initial-position")

        # Sending 5 messages before consumer creation.
        # These should be received with initial_position set to Earliest but not with Latest.
        for i in range(5):
            producer.send(b"hello-%d" % i)

        consumer = client.subscribe(
            "consumer-initial-position",
            "my-sub",
            consumer_type=ConsumerType.Shared,
            initial_position=InitialPosition.Earliest,
        )

        # Sending 5 other messages that should be received regardless of the initial_position.
        for i in range(5, 10):
            producer.send(b"hello-%d" % i)

        for i in range(10):
            msg = consumer.receive(TM)
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        consumer.unsubscribe()
        client.close()

    def test_consumer_queue_size_is_zero(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe(
            "my-python-topic-consumer-init-queue-size-is-zero",
            "my-sub",
            consumer_type=ConsumerType.Shared,
            receiver_queue_size=0,
            initial_position=InitialPosition.Earliest,
        )
        producer = client.create_producer("my-python-topic-consumer-init-queue-size-is-zero")
        producer.send(b"hello")
        time.sleep(0.1)
        msg = consumer.receive()
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        consumer.unsubscribe()
        client.close()

    def test_message_properties(self):
        client = Client(self.serviceUrl)
        topic = "my-python-test-message-properties"
        consumer = client.subscribe(
            topic=topic, subscription_name="my-subscription", schema=pulsar.schema.StringSchema()
        )
        producer = client.create_producer(topic=topic, schema=pulsar.schema.StringSchema())
        producer.send("hello", properties={"a": "1", "b": "2"})

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.value(), "hello")
        self.assertEqual(msg.properties(), {"a": "1", "b": "2"})

        consumer.unsubscribe()
        client.close()

    def test_tls_auth(self):
        client = Client(
            self.serviceUrlTls,
            tls_trust_certs_file_path=CERTS_DIR + "cacert.pem",
            tls_allow_insecure_connection=False,
            authentication=AuthenticationTLS(CERTS_DIR + "client-cert.pem", CERTS_DIR + "client-key.pem"),
        )

        topic = "my-python-topic-tls-auth-" + str(time.time())
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer(topic)
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        client.close()

    def test_tls_auth2(self):
        authPlugin = "org.apache.pulsar.client.impl.auth.AuthenticationTls"
        authParams = "tlsCertFile:%s/client-cert.pem,tlsKeyFile:%s/client-key.pem" % (CERTS_DIR, CERTS_DIR)

        client = Client(
            self.serviceUrlTls,
            tls_trust_certs_file_path=CERTS_DIR + "cacert.pem",
            tls_allow_insecure_connection=False,
            authentication=Authentication(authPlugin, authParams),
        )

        topic = "my-python-topic-tls-auth-2-" + str(time.time())
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer(topic)
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        client.close()

    def test_encryption(self):
        publicKeyPath = CERTS_DIR + "public-key.client-rsa.pem"
        privateKeyPath = CERTS_DIR + "private-key.client-rsa.pem"
        crypto_key_reader = CryptoKeyReader(publicKeyPath, privateKeyPath)
        client = Client(self.serviceUrl)
        topic = "my-python-test-end-to-end-encryption"
        consumer = client.subscribe(
            topic=topic, subscription_name="my-subscription", crypto_key_reader=crypto_key_reader
        )
        producer = client.create_producer(
            topic=topic, encryption_key="client-rsa.pem", crypto_key_reader=crypto_key_reader
        )
        reader = client.create_reader(
            topic=topic, start_message_id=MessageId.earliest, crypto_key_reader=crypto_key_reader
        )
        producer.send(b"hello")
        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.value(), b"hello")
        consumer.unsubscribe()

        msg = reader.read_next(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            reader.read_next(100)

        reader.close()

        client.close()

    def test_tls_auth3(self):
        authPlugin = "tls"
        authParams = "tlsCertFile:%s/client-cert.pem,tlsKeyFile:%s/client-key.pem" % (CERTS_DIR, CERTS_DIR)

        client = Client(
            self.serviceUrlTls,
            tls_trust_certs_file_path=CERTS_DIR + "cacert.pem",
            tls_allow_insecure_connection=False,
            authentication=Authentication(authPlugin, authParams),
        )

        topic = "my-python-topic-tls-auth-3-" + str(time.time())
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer(topic)
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        client.close()

    def test_auth_junk_params(self):
        authPlugin = "someoldjunk.so"
        authParams = "blah"
        client = Client(
            self.serviceUrlTls,
            tls_trust_certs_file_path=CERTS_DIR + "cacert.pem",
            tls_allow_insecure_connection=False,
            authentication=Authentication(authPlugin, authParams),
        )

        with self.assertRaises(pulsar.ConnectError):
            client.subscribe("my-python-topic-auth-junk-params", "my-sub", consumer_type=ConsumerType.Shared)

    def test_message_listener(self):
        client = Client(self.serviceUrl)

        received_messages = []

        def listener(consumer, msg):
            print("Got message: %s" % msg)
            received_messages.append(msg)
            consumer.acknowledge(msg)

        client.subscribe(
            "my-python-topic-listener", "my-sub", consumer_type=ConsumerType.Exclusive, message_listener=listener
        )
        producer = client.create_producer("my-python-topic-listener")
        producer.send(b"hello-1")
        producer.send(b"hello-2")
        producer.send(b"hello-3")

        time.sleep(0.1)
        self.assertEqual(len(received_messages), 3)
        self.assertEqual(received_messages[0].data(), b"hello-1")
        self.assertEqual(received_messages[1].data(), b"hello-2")
        self.assertEqual(received_messages[2].data(), b"hello-3")
        client.close()

    def test_consumer_is_connected(self):
        client = Client(self.serviceUrl)
        topic = "test_consumer_is_connected"
        sub = "sub"
        consumer = client.subscribe(topic, sub)
        self.assertTrue(consumer.is_connected())
        consumer.close()
        self.assertFalse(consumer.is_connected())
        client.close()

    def test_reader_simple(self):
        client = Client(self.serviceUrl)
        reader = client.create_reader("my-python-topic-reader-simple", MessageId.earliest)

        producer = client.create_producer("my-python-topic-reader-simple")
        producer.send(b"hello")

        msg = reader.read_next(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            reader.read_next(100)

        reader.close()
        client.close()

    def test_reader_on_last_message(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer("my-python-topic-reader-on-last-message")

        for i in range(10):
            producer.send(b"hello-%d" % i)

        reader = client.create_reader("my-python-topic-reader-on-last-message", MessageId.latest)

        for i in range(10, 20):
            producer.send(b"hello-%d" % i)

        for i in range(10, 20):
            msg = reader.read_next(TM)
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)

        reader.close()
        client.close()

    def test_reader_on_specific_message(self):
        num_of_msgs = 10
        client = Client(self.serviceUrl)
        producer = client.create_producer("my-python-topic-reader-on-specific-message")

        for i in range(num_of_msgs):
            producer.send(b"hello-%d" % i)

        reader1 = client.create_reader("my-python-topic-reader-on-specific-message", MessageId.earliest)

        for i in range(num_of_msgs // 2):
            msg = reader1.read_next(TM)
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)
            last_msg_id = msg.message_id()
            last_msg_idx = i

        reader2 = client.create_reader("my-python-topic-reader-on-specific-message", last_msg_id)

        # The reset would be effectively done on the next position relative to reset.
        # When available, we should test this behaviour with `startMessageIdInclusive` opt.
        from_msg_idx = last_msg_idx
        for i in range(from_msg_idx + 1, num_of_msgs):
            msg = reader2.read_next(TM)
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)

        reader1.close()
        reader2.close()
        client.close()

    def test_reader_on_specific_message_with_batches(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer(
            "my-python-topic-reader-on-specific-message-with-batches",
            batching_enabled=True,
            batching_max_publish_delay_ms=1000,
        )

        for i in range(10):
            producer.send_async(b"hello-%d" % i, None)

        # Send one sync message to make sure everything was published
        producer.send(b"hello-10")

        reader1 = client.create_reader("my-python-topic-reader-on-specific-message-with-batches", MessageId.earliest)

        for i in range(5):
            msg = reader1.read_next(TM)
            last_msg_id = msg.message_id()

        reader2 = client.create_reader("my-python-topic-reader-on-specific-message-with-batches", last_msg_id)

        for i in range(5, 11):
            msg = reader2.read_next(TM)
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)

        reader1.close()
        reader2.close()
        client.close()

    def test_reader_on_partitioned_topic(self):
        num_of_msgs = 100
        topic_name = "public/default/my-python-topic-test_reader_on_partitioned_topic"
        url1 = self.adminUrl + "/admin/v2/persistent/" + topic_name + "/partitions"
        doHttpPut(url1, "4")

        client = Client(self.serviceUrl)
        producer = client.create_producer(topic_name)

        send_array = []
        for i in range(num_of_msgs):
            data = b"hello-%d" % i
            producer.send(data)
            send_array.append(data)

        reader = client.create_reader(topic_name, MessageId.earliest)

        read_array = []
        for i in range(num_of_msgs):
            msg = reader.read_next(TM)
            self.assertTrue(msg)
            read_array.append(msg.data())

        self.assertListEqual(sorted(send_array), sorted(read_array))
        reader.close()
        client.close()

    def test_reader_is_connected(self):
        client = Client(self.serviceUrl)
        topic = "test_reader_is_connected"
        reader = client.create_reader(topic, MessageId.earliest)
        self.assertTrue(reader.is_connected())
        reader.close()
        self.assertFalse(reader.is_connected())
        client.close()

    def test_producer_sequence_after_reconnection(self):
        # Enable deduplication on namespace
        doHttpPost(self.adminUrl + "/admin/v2/namespaces/public/default/deduplication", "true")
        client = Client(self.serviceUrl)

        topic = "my-python-test-producer-sequence-after-reconnection-" + str(time.time())

        producer = client.create_producer(topic, producer_name="my-producer-name")
        self.assertEqual(producer.last_sequence_id(), -1)

        for i in range(10):
            producer.send(b"hello-%d" % i)
            self.assertEqual(producer.last_sequence_id(), i)

        producer.close()

        producer = client.create_producer(topic, producer_name="my-producer-name")
        self.assertEqual(producer.last_sequence_id(), 9)

        for i in range(10, 20):
            producer.send(b"hello-%d" % i)
            self.assertEqual(producer.last_sequence_id(), i)

        client.close()

        doHttpPost(self.adminUrl + "/admin/v2/namespaces/public/default/deduplication", "false")

    def test_producer_deduplication(self):
        # Enable deduplication on namespace
        doHttpPost(self.adminUrl + "/admin/v2/namespaces/public/default/deduplication", "true")
        client = Client(self.serviceUrl)

        topic = "my-python-test-producer-deduplication-" + str(time.time())

        producer = client.create_producer(topic, producer_name="my-producer-name")
        self.assertEqual(producer.last_sequence_id(), -1)

        consumer = client.subscribe(topic, "my-sub")

        producer.send(b"hello-0", sequence_id=0)
        producer.send(b"hello-1", sequence_id=1)
        producer.send(b"hello-2", sequence_id=2)
        self.assertEqual(producer.last_sequence_id(), 2)

        # Repeat the messages and verify they're not received by consumer
        producer.send(b"hello-1", sequence_id=1)
        producer.send(b"hello-2", sequence_id=2)
        self.assertEqual(producer.last_sequence_id(), 2)

        for i in range(3):
            msg = consumer.receive(TM)
            self.assertEqual(msg.data(), b"hello-%d" % i)
            consumer.acknowledge(msg)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        producer.close()

        producer = client.create_producer(topic, producer_name="my-producer-name")
        self.assertEqual(producer.last_sequence_id(), 2)

        # Repeat the messages and verify they're not received by consumer
        producer.send(b"hello-1", sequence_id=1)
        producer.send(b"hello-2", sequence_id=2)
        self.assertEqual(producer.last_sequence_id(), 2)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        client.close()

        doHttpPost(self.adminUrl + "/admin/v2/namespaces/public/default/deduplication", "false")

    def test_producer_routing_mode(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer(
            "my-python-test-producer", message_routing_mode=PartitionsRoutingMode.UseSinglePartition
        )
        producer.send(b"test")
        client.close()

    def test_message_argument_errors(self):
        client = Client(self.serviceUrl)
        topic = "my-python-test-producer"
        producer = client.create_producer(topic)

        content = "test".encode("utf-8")

        self._check_type_error(lambda: producer.send(5))
        self._check_value_error(lambda: producer.send(content, properties="test"))
        self._check_value_error(lambda: producer.send(content, partition_key=5))
        self._check_value_error(lambda: producer.send(content, sequence_id="test"))
        self._check_value_error(lambda: producer.send(content, replication_clusters=5))
        self._check_value_error(lambda: producer.send(content, disable_replication="test"))
        self._check_value_error(lambda: producer.send(content, event_timestamp="test"))
        self._check_value_error(lambda: producer.send(content, deliver_at="test"))
        self._check_value_error(lambda: producer.send(content, deliver_after="test"))
        client.close()

    def test_client_argument_errors(self):
        self._check_value_error(lambda: Client(None))
        self._check_value_error(lambda: Client(self.serviceUrl, authentication="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, operation_timeout_seconds="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, io_threads="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, message_listener_threads="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, concurrent_lookup_requests="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, use_tls="test"))
        self._check_value_error(lambda: Client(self.serviceUrl, tls_trust_certs_file_path=5))
        self._check_value_error(lambda: Client(self.serviceUrl, tls_allow_insecure_connection="test"))

    def test_producer_argument_errors(self):
        client = Client(self.serviceUrl)

        self._check_value_error(lambda: client.create_producer(None))

        topic = "my-python-test-producer"

        self._check_value_error(lambda: client.create_producer(topic, producer_name=5))
        self._check_value_error(lambda: client.create_producer(topic, initial_sequence_id="test"))
        self._check_value_error(lambda: client.create_producer(topic, send_timeout_millis="test"))
        self._check_value_error(lambda: client.create_producer(topic, compression_type=None))
        self._check_value_error(lambda: client.create_producer(topic, max_pending_messages="test"))
        self._check_value_error(lambda: client.create_producer(topic, block_if_queue_full="test"))
        self._check_value_error(lambda: client.create_producer(topic, batching_enabled="test"))
        self._check_value_error(lambda: client.create_producer(topic, batching_enabled="test"))
        self._check_value_error(lambda: client.create_producer(topic, batching_max_allowed_size_in_bytes="test"))
        self._check_value_error(lambda: client.create_producer(topic, batching_max_publish_delay_ms="test"))
        client.close()

    def test_consumer_argument_errors(self):
        client = Client(self.serviceUrl)

        topic = "my-python-test-producer"
        sub_name = "my-sub-name"

        self._check_value_error(lambda: client.subscribe(None, sub_name))
        self._check_value_error(lambda: client.subscribe(topic, None))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, consumer_type=None))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, receiver_queue_size="test"))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, consumer_name=5))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, unacked_messages_timeout_ms="test"))
        self._check_value_error(lambda: client.subscribe(topic, sub_name, broker_consumer_stats_cache_time_ms="test"))
        client.close()

    def test_reader_argument_errors(self):
        client = Client(self.serviceUrl)
        topic = "my-python-test-producer"

        # This should not raise exception
        client.create_reader(topic, MessageId.earliest)

        self._check_value_error(lambda: client.create_reader(None, MessageId.earliest))
        self._check_value_error(lambda: client.create_reader(topic, None))
        self._check_value_error(lambda: client.create_reader(topic, MessageId.earliest, receiver_queue_size="test"))
        self._check_value_error(lambda: client.create_reader(topic, MessageId.earliest, reader_name=5))
        client.close()

    def test_get_last_message_id(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe(
            "persistent://public/default/topic_name_test", "topic_name_test_sub", consumer_type=ConsumerType.Shared
        )
        producer = client.create_producer("persistent://public/default/topic_name_test")
        msg_id = producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertEqual(msg.message_id(), msg_id)
        client.close()

    def test_publish_compact_and_consume(self):
        client = Client(self.serviceUrl)
        topic = "compaction_%s" % (uuid.uuid4())
        producer = client.create_producer(topic, producer_name="my-producer-name", batching_enabled=False)
        self.assertEqual(producer.last_sequence_id(), -1)
        consumer = client.subscribe(topic, "my-sub1", is_read_compacted=True)
        consumer.close()
        consumer2 = client.subscribe(topic, "my-sub2", is_read_compacted=False)

        # producer create 2 messages with same key.
        producer.send(b"hello-0", partition_key="key0")
        producer.send(b"hello-1", partition_key="key0")
        producer.close()

        # issue compact command, and wait success
        url = "%s/admin/v2/persistent/public/default/%s/compaction" % (self.adminUrl, topic)
        doHttpPut(url, "")
        while True:
            s = doHttpGet(url).decode("utf-8")
            if "RUNNING" in s:
                print(s)
                print("Compact still running")
                time.sleep(0.2)
            else:
                print(s)
                print("Compact Complete now")
                self.assertTrue("SUCCESS" in s)
                break

        # after compaction completes the compacted ledger is recorded
        # as a property of a cursor. As persisting the cursor is async
        # and we don't wait for the acknowledgement of the acknowledgement,
        # there may be a race if we try to read the compacted ledger immediately.
        # therefore wait a second to allow the compacted ledger to be updated on
        # the broker.
        time.sleep(1.0)

        # after compact, consumer with `is_read_compacted=True`, expected read only the second message for same key.
        consumer1 = client.subscribe(topic, "my-sub1", is_read_compacted=True)
        msg0 = consumer1.receive(TM)
        self.assertEqual(msg0.data(), b"hello-1")
        consumer1.acknowledge(msg0)
        consumer1.close()

        # ditto for reader
        reader1 = client.create_reader(topic, MessageId.earliest, is_read_compacted=True)
        msg0 = reader1.read_next(TM)
        self.assertEqual(msg0.data(), b"hello-1")
        reader1.close()

        # after compact, consumer with `is_read_compacted=False`, expected read 2 messages for same key.
        msg0 = consumer2.receive(TM)
        self.assertEqual(msg0.data(), b"hello-0")
        consumer2.acknowledge(msg0)
        msg1 = consumer2.receive(TM)
        self.assertEqual(msg1.data(), b"hello-1")
        consumer2.acknowledge(msg1)
        consumer2.close()

        # ditto for reader
        reader2 = client.create_reader(topic, MessageId.earliest, is_read_compacted=False)
        msg0 = reader2.read_next(TM)
        self.assertEqual(msg0.data(), b"hello-0")
        msg1 = reader2.read_next(TM)
        self.assertEqual(msg1.data(), b"hello-1")
        reader2.close()
        client.close()

    def test_reader_has_message_available(self):
        # create client, producer, reader
        client = Client(self.serviceUrl)
        producer = client.create_producer("my-python-topic-reader-has-message-available")
        reader = client.create_reader("my-python-topic-reader-has-message-available", MessageId.latest)

        # before produce data, expected not has message available
        self.assertFalse(reader.has_message_available())

        for i in range(10):
            producer.send(b"hello-%d" % i)

        # produced data, expected has message available
        self.assertTrue(reader.has_message_available())

        for i in range(10):
            msg = reader.read_next(TM)
            self.assertTrue(msg)
            self.assertEqual(msg.data(), b"hello-%d" % i)

        # consumed all data, expected not has message available
        self.assertFalse(reader.has_message_available())

        for i in range(10, 20):
            producer.send(b"hello-%d" % i)

        # produced data again, expected has message available
        self.assertTrue(reader.has_message_available())
        reader.close()
        producer.close()
        client.close()

    def test_seek(self):
        client = Client(self.serviceUrl)
        topic = "my-python-topic-seek-" + str(time.time())
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer(topic)

        for i in range(100):
            if i > 0:
                time.sleep(0.02)
            producer.send(b"hello-%d" % i)

        ids = []
        timestamps = []
        for i in range(100):
            msg = consumer.receive(TM)
            self.assertEqual(msg.data(), b"hello-%d" % i)
            ids.append(msg.message_id())
            timestamps.append(msg.publish_timestamp())
            consumer.acknowledge(msg)

        # seek, and after reconnect, expected receive first message.
        consumer.seek(MessageId.earliest)
        time.sleep(0.5)
        msg = consumer.receive(TM)
        self.assertEqual(msg.data(), b"hello-0")

        # seek on messageId
        consumer.seek(ids[50])
        time.sleep(0.5)
        msg = consumer.receive(TM)
        self.assertEqual(msg.data(), b"hello-51")

        # ditto, but seek on timestamp
        consumer.seek(timestamps[42])
        time.sleep(0.5)
        msg = consumer.receive(TM)
        self.assertEqual(msg.data(), b"hello-42")

        # repeat with reader
        reader = client.create_reader(topic, MessageId.latest)
        with self.assertRaises(pulsar.Timeout):
            reader.read_next(100)

        # earliest
        reader.seek(MessageId.earliest)
        time.sleep(0.5)
        msg = reader.read_next(TM)
        self.assertEqual(msg.data(), b"hello-0")
        msg = reader.read_next(TM)
        self.assertEqual(msg.data(), b"hello-1")

        # seek on messageId
        reader.seek(ids[33])
        time.sleep(0.5)
        msg = reader.read_next(TM)
        self.assertEqual(msg.data(), b"hello-34")
        msg = reader.read_next(TM)
        self.assertEqual(msg.data(), b"hello-35")

        # seek on timestamp
        reader.seek(timestamps[79])
        time.sleep(0.5)
        msg = reader.read_next(TM)
        self.assertEqual(msg.data(), b"hello-79")
        msg = reader.read_next(TM)
        self.assertEqual(msg.data(), b"hello-80")

        reader.close()
        client.close()

    def test_seek_inclusive(self):
        client = Client(self.serviceUrl)
        topic = "my-python-topic-seek-inclusive-" + str(time.time())
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared, start_message_id_inclusive=True)
        producer = client.create_producer(topic)

        for i in range(100):
            if i > 0:
                time.sleep(0.02)
            producer.send(b"hello-%d" % i)

        ids = []
        for i in range(100):
            msg = consumer.receive(TM)
            self.assertEqual(msg.data(), b"hello-%d" % i)
            ids.append(msg.message_id())
            consumer.acknowledge(msg)

        # seek, and after reconnect, expected receive first message.
        consumer.seek(MessageId.earliest)
        time.sleep(0.5)
        msg = consumer.receive(TM)
        self.assertEqual(msg.data(), b"hello-0")

        # seek on messageId
        consumer.seek(ids[50])
        time.sleep(0.5)
        msg = consumer.receive(TM)
        self.assertEqual(msg.data(), b"hello-50")
        client.close()

    def test_v2_topics(self):
        self._v2_topics(self.serviceUrl)

    def test_v2_topics_http(self):
        self._v2_topics(self.adminUrl)

    def _v2_topics(self, url):
        client = Client(url)
        consumer = client.subscribe("my-v2-topic-producer-consumer", "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer("my-v2-topic-producer-consumer")
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        consumer.acknowledge(msg)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        client.close()

    def test_topics_consumer(self):
        client = Client(self.serviceUrl)
        topic1 = "persistent://public/default/my-python-topics-consumer-1"
        topic2 = "persistent://public/default/my-python-topics-consumer-2"
        topic3 = "persistent://public/default-2/my-python-topics-consumer-3"  # topic from different namespace
        topics = [topic1, topic2, topic3]

        url1 = self.adminUrl + "/admin/v2/persistent/public/default/my-python-topics-consumer-1/partitions"
        url2 = self.adminUrl + "/admin/v2/persistent/public/default/my-python-topics-consumer-2/partitions"
        url3 = self.adminUrl + "/admin/v2/persistent/public/default-2/my-python-topics-consumer-3/partitions"

        doHttpPut(url1, "2")
        doHttpPut(url2, "3")
        doHttpPut(url3, "4")

        producer1 = client.create_producer(topic1)
        producer2 = client.create_producer(topic2)
        producer3 = client.create_producer(topic3)

        consumer = client.subscribe(
            topics, "my-topics-consumer-sub", consumer_type=ConsumerType.Shared, receiver_queue_size=10
        )

        for i in range(100):
            producer1.send(b"hello-1-%d" % i)

        for i in range(100):
            producer2.send(b"hello-2-%d" % i)

        for i in range(100):
            producer3.send(b"hello-3-%d" % i)

        for i in range(300):
            msg = consumer.receive(TM)
            consumer.acknowledge(msg)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)
        client.close()

    def test_topics_pattern_consumer(self):
        import re

        client = Client(self.serviceUrl)

        topics_pattern = "persistent://public/default/my-python-pattern-consumer.*"

        topic1 = "persistent://public/default/my-python-pattern-consumer-1"
        topic2 = "persistent://public/default/my-python-pattern-consumer-2"
        topic3 = "persistent://public/default/my-python-pattern-consumer-3"

        url1 = self.adminUrl + "/admin/v2/persistent/public/default/my-python-pattern-consumer-1/partitions"
        url2 = self.adminUrl + "/admin/v2/persistent/public/default/my-python-pattern-consumer-2/partitions"
        url3 = self.adminUrl + "/admin/v2/persistent/public/default/my-python-pattern-consumer-3/partitions"

        doHttpPut(url1, "2")
        doHttpPut(url2, "3")
        doHttpPut(url3, "4")

        producer1 = client.create_producer(topic1)
        producer2 = client.create_producer(topic2)
        producer3 = client.create_producer(topic3)

        consumer = client.subscribe(
            re.compile(topics_pattern),
            "my-pattern-consumer-sub",
            consumer_type=ConsumerType.Shared,
            receiver_queue_size=10,
            pattern_auto_discovery_period=1,
        )

        # wait enough time to trigger auto discovery
        time.sleep(2)

        for i in range(100):
            producer1.send(b"hello-1-%d" % i)

        for i in range(100):
            producer2.send(b"hello-2-%d" % i)

        for i in range(100):
            producer3.send(b"hello-3-%d" % i)

        for i in range(300):
            msg = consumer.receive(TM)
            consumer.acknowledge(msg)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)
        client.close()

    def test_batch_receive(self):
        client = Client(self.serviceUrl)
        topic = "my-python-topic-batch-receive-" + str(time.time())
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared,
                                    start_message_id_inclusive=True, batch_receive_policy=ConsumerBatchReceivePolicy(10, -1, -1))
        producer = client.create_producer(topic)


        for i in range(10):
            if i > 0:
                time.sleep(0.02)
            producer.send(b"hello-%d" % i)

        msgs = consumer.batch_receive()
        i = 0
        for msg in msgs:
            self.assertEqual(msg.data(), b"hello-%d" % i)
            i += 1

        client.close()

    def test_message_id(self):
        s = MessageId.earliest.serialize()
        self.assertEqual(MessageId.deserialize(s), MessageId.earliest)

        s = MessageId.latest.serialize()
        self.assertEqual(MessageId.deserialize(s), MessageId.latest)

    def test_get_topics_partitions(self):
        client = Client(self.serviceUrl)
        topic_partitioned = "persistent://public/default/test_get_topics_partitions"
        topic_non_partitioned = "persistent://public/default/test_get_topics_not-partitioned"

        url1 = self.adminUrl + "/admin/v2/persistent/public/default/test_get_topics_partitions/partitions"
        doHttpPut(url1, "3")

        self.assertEqual(
            client.get_topic_partitions(topic_partitioned),
            [
                "persistent://public/default/test_get_topics_partitions-partition-0",
                "persistent://public/default/test_get_topics_partitions-partition-1",
                "persistent://public/default/test_get_topics_partitions-partition-2",
            ],
        )

        self.assertEqual(client.get_topic_partitions(topic_non_partitioned), [topic_non_partitioned])
        client.close()

    def test_token_auth(self):
        with open(".test-token.txt") as tf:
            token = tf.read().strip()

        # Use adminUrl to test both HTTP request and binary protocol
        client = Client(self.adminUrl, authentication=AuthenticationToken(token))

        consumer = client.subscribe(
            "persistent://private/auth/my-python-topic-token-auth", "my-sub", consumer_type=ConsumerType.Shared
        )
        producer = client.create_producer("persistent://private/auth/my-python-topic-token-auth")
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        client.close()

    def test_token_auth_supplier(self):
        def read_token():
            with open(".test-token.txt") as tf:
                return tf.read().strip()

        client = Client(self.serviceUrl, authentication=AuthenticationToken(read_token))
        consumer = client.subscribe(
            "persistent://private/auth/my-python-topic-token-auth", "my-sub", consumer_type=ConsumerType.Shared
        )
        producer = client.create_producer("persistent://private/auth/my-python-topic-token-auth")
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        client.close()

    def test_producer_consumer_zstd(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe(
            "my-python-topic-producer-consumer-zstd", "my-sub", consumer_type=ConsumerType.Shared
        )
        producer = client.create_producer(
            "my-python-topic-producer-consumer-zstd", compression_type=CompressionType.ZSTD
        )
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)

        consumer.unsubscribe()
        client.close()

    def test_client_reference_deleted(self):
        def get_producer():
            cl = Client(self.serviceUrl)
            return cl.create_producer(topic="foobar")

        producer = get_producer()
        producer.send(b"test_payload")

    #####

    def test_get_topic_name(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe(
            "persistent://public/default/topic_name_test", "topic_name_test_sub", consumer_type=ConsumerType.Shared
        )
        producer = client.create_producer("persistent://public/default/topic_name_test")
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertEqual(msg.topic_name(), "persistent://public/default/topic_name_test")
        client.close()

    def test_get_partitioned_topic_name(self):
        client = Client(self.serviceUrl)
        url1 = self.adminUrl + "/admin/v2/persistent/public/default/partitioned_topic_name_test/partitions"
        doHttpPut(url1, "3")

        partitions = [
            "persistent://public/default/partitioned_topic_name_test-partition-0",
            "persistent://public/default/partitioned_topic_name_test-partition-1",
            "persistent://public/default/partitioned_topic_name_test-partition-2",
        ]
        self.assertEqual(
            client.get_topic_partitions("persistent://public/default/partitioned_topic_name_test"), partitions
        )

        consumer = client.subscribe(
            "persistent://public/default/partitioned_topic_name_test",
            "partitioned_topic_name_test_sub",
            consumer_type=ConsumerType.Shared,
        )
        producer = client.create_producer("persistent://public/default/partitioned_topic_name_test")
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg.topic_name() in partitions)
        client.close()

    def test_shutdown_client(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer("persistent://public/default/partitioned_topic_name_test")
        producer.send(b"hello")
        client.shutdown()

        try:
            producer.send(b"hello")
            self.assertTrue(False)
        except pulsar.PulsarException:
            # Expected
            pass

    def test_listener_name_client(self):
        client = Client(self.serviceUrl, listener_name='test')
        try:
            producer = client.create_producer("persistent://public/default/partitioned_topic_name_test")
            self.fail()
        except pulsar.PulsarException:
            # Expected
            pass
        client.close()

    def test_negative_acks(self):
        client = Client(self.serviceUrl)
        consumer = client.subscribe(
            "test_negative_acks", "test", schema=pulsar.schema.StringSchema(), negative_ack_redelivery_delay_ms=1000
        )
        producer = client.create_producer("test_negative_acks", schema=pulsar.schema.StringSchema())
        for i in range(10):
            producer.send_async("hello-%d" % i, callback=None)

        producer.flush()

        for i in range(10):
            msg = consumer.receive()
            self.assertEqual(msg.value(), "hello-%d" % i)
            consumer.negative_acknowledge(msg)

        for i in range(10):
            msg = consumer.receive()
            self.assertEqual(msg.value(), "hello-%d" % i)
            consumer.acknowledge(msg)

        with self.assertRaises(pulsar.Timeout):
            consumer.receive(100)
        client.close()

    def test_connect_timeout(self):
        client = pulsar.Client(
            service_url="pulsar://192.0.2.1:1234",
            connection_timeout_ms=1000,  # 1 second
        )
        t1 = time.time()
        try:
            producer = client.create_producer("test_connect_timeout")
            self.fail("create_producer should not succeed")
        except pulsar.ConnectError as expected:
            print("expected error: {} when create producer".format(expected))
        t2 = time.time()
        self.assertGreater(t2 - t1, 1.0)
        self.assertLess(t2 - t1, 1.5)  # 1.5 seconds is long enough
        client.close()

    def test_json_schema_encode(self):
        schema = JsonSchema(TestRecord)
        record = TestRecord(a=1, b=2)
        # Ensure that encoding a JsonSchema more than once works and produces the same result
        first_encode = schema.encode(record)
        second_encode = schema.encode(record)
        self.assertEqual(first_encode, second_encode)

    def test_configure_log_level(self):
        client = pulsar.Client(
            service_url="pulsar://localhost:6650",
            logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Debug)
        )

        producer = client.create_producer(
            topic='test_log_level'
        )

        producer.send(b'hello')

    def test_configure_log_to_file(self):
        client = pulsar.Client(
            service_url="pulsar://localhost:6650",
            logger=pulsar.FileLogger(pulsar.LoggerLevel.Debug, 'test.log')
        )

        producer = client.create_producer(
            topic='test_log_to_file'
        )

        producer.send(b'hello')

    def test_logger_thread_leaks(self):
        def _do_connect(close):
            logger = logging.getLogger(str(threading.current_thread().ident))
            logger.setLevel(logging.INFO)
            client = pulsar.Client(
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

    def test_chunking(self):
        client = Client(self.serviceUrl)
        data_size = 10 * 1024 * 1024
        producer = client.create_producer(
            'test_chunking',
            chunking_enabled=True
        )

        consumer = client.subscribe('test_chunking', "my-subscription",
                                    max_pending_chunked_message=10,
                                    auto_ack_oldest_chunked_message_on_queue_full=False
                                    )

        producer.send(bytes(bytearray(os.urandom(data_size))), None)
        msg = consumer.receive(TM)
        self.assertEqual(len(msg.data()), data_size)

    def test_invalid_chunking_config(self):
        client = Client(self.serviceUrl)

        self._check_value_error(lambda: client.create_producer(
            'test_invalid_chunking_config',
            chunking_enabled=True,
            batching_enabled=True
        ))

    def _check_value_error(self, fun):
        with self.assertRaises(ValueError):
            fun()

    def _check_type_error(self, fun):
        with self.assertRaises(TypeError):
            fun()

    def _test_basic_auth(self, id, auth):
        client = Client(self.adminUrl, authentication=auth)

        topic = "persistent://private/auth/my-python-topic-basic-auth-" + str(id)
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        producer = client.create_producer(topic)
        producer.send(b"hello")

        msg = consumer.receive(TM)
        self.assertTrue(msg)
        self.assertEqual(msg.data(), b"hello")
        client.close()

    def test_basic_auth(self):
        username = "admin"
        password = "123456"
        self._test_basic_auth(0, AuthenticationBasic(username, password))
        self._test_basic_auth(1, AuthenticationBasic(
            auth_params_string='{{"username": "{}","password": "{}"}}'.format(username, password)
        ))

    def test_basic_auth_method(self):
        username = "admin"
        password = "123456"
        self._test_basic_auth(2, AuthenticationBasic(username, password, 'basic'))
        with self.assertRaises(pulsar.AuthorizationError):
            self._test_basic_auth(3, AuthenticationBasic(username, password, 'unknown'))
        self._test_basic_auth(4, AuthenticationBasic(
            auth_params_string='{{"username": "{}","password": "{}", "method": "basic"}}'.format(username, password)
        ))
        with self.assertRaises(pulsar.AuthorizationError):
            self._test_basic_auth(5, AuthenticationBasic(
                auth_params_string='{{"username": "{}","password": "{}", "method": "unknown"}}'.format(username, password)
            ))

    def test_invalid_basic_auth(self):
        username = "invalid"
        password = "123456"
        client = Client(self.adminUrl, authentication=AuthenticationBasic(username, password))
        topic = "persistent://private/auth/my-python-topic-invalid-basic-auth"
        with self.assertRaises(pulsar.ConnectError):
            client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        client = Client(self.adminUrl, authentication=AuthenticationBasic(
            auth_params_string='{{"username": "{}","password": "{}"}}'.format(username, password)
        ))
        with self.assertRaises(pulsar.ConnectError):
            client.subscribe(topic, "my-sub", consumer_type=ConsumerType.Shared)
        with self.assertRaises(RuntimeError):
            AuthenticationBasic(auth_params_string='invalid auth params')

    def test_send_async_no_deadlock(self):
        client = Client(self.serviceUrl)
        producer = client.create_producer('test_send_async_no_deadlock')

        def send_callback(res, msg):
            print(f"Message '{msg}' published res={res}")

        for i in range(30):
            producer.send_async(f"Hello-{i}".encode('utf-8'), callback=send_callback)

        producer.flush()
        client.close()

    def test_keyshare_policy(self):
        with self.assertRaises(ValueError):
            # Raise error because sticky ranges are not provided.
            pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                allow_out_of_order_delivery=False,
            )

        expected_key_shared_mode = pulsar.KeySharedMode.Sticky
        expected_allow_out_of_order_delivery = True
        expected_sticky_ranges = [(0, 100), (101,200)]
        consumer_key_shared_policy = pulsar.ConsumerKeySharedPolicy(
            key_shared_mode=expected_key_shared_mode,
            allow_out_of_order_delivery=expected_allow_out_of_order_delivery,
            sticky_ranges=expected_sticky_ranges
        )

        self.assertEqual(consumer_key_shared_policy.key_shared_mode, expected_key_shared_mode)
        self.assertEqual(consumer_key_shared_policy.allow_out_of_order_delivery, expected_allow_out_of_order_delivery)
        self.assertEqual(consumer_key_shared_policy.sticky_ranges, expected_sticky_ranges)

    def test_keyshared_invalid_sticky_ranges(self):
        client = Client(self.serviceUrl)
        topic = "my-python-topic-keyshare-invalid-" + str(time.time())
        with self.assertRaises(ValueError):
            consumer_key_shared_policy = pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                allow_out_of_order_delivery=False,
                sticky_ranges=[(0,65536)]
            )
            client.subscribe(topic, "my-sub", consumer_type=ConsumerType.KeyShared,
                             start_message_id_inclusive=True,
                             key_shared_policy=consumer_key_shared_policy)

        with self.assertRaises(ValueError):
            consumer_key_shared_policy = pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                allow_out_of_order_delivery=False,
                sticky_ranges=[(0, 100), (50, 150)]
            )
            client.subscribe(topic, "my-sub", consumer_type=ConsumerType.KeyShared,
                             start_message_id_inclusive=True,
                             key_shared_policy=consumer_key_shared_policy)

    def test_keyshared_autosplit(self):
        client = Client(self.serviceUrl)
        topic = "my-python-topic-keyshare-autosplit-" + str(time.time())
        consumer_key_shared_policy = pulsar.ConsumerKeySharedPolicy(
            key_shared_mode=pulsar.KeySharedMode.AutoSplit,
            allow_out_of_order_delivery=True,
        )
        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.KeyShared, consumer_name = 'con-1',
                                    start_message_id_inclusive=True, key_shared_policy=consumer_key_shared_policy)
        consumer2 = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.KeyShared, consumer_name = 'con-2',
                                    start_message_id_inclusive=True, key_shared_policy=consumer_key_shared_policy)
        producer = client.create_producer(topic)

        for i in range(10):
            if i > 0:
                time.sleep(0.02)
            producer.send(b"hello-%d" % i)

        msgs = []
        while True:
            try:
                msg = consumer.receive(100)
            except pulsar.Timeout:
                break
            msgs.append(msg)
            consumer.acknowledge(msg)

        while True:
            try:
                msg = consumer2.receive(100)
            except pulsar.Timeout:
                break
            msgs.append(msg)
            consumer2.acknowledge(msg)

        self.assertEqual(len(msgs), 10)
        client.close()

    def test_sticky_autosplit(self):
        client = Client(self.serviceUrl)
        topic = "my-python-topic-keyshare-sticky-" + str(time.time())
        consumer_key_shared_policy = pulsar.ConsumerKeySharedPolicy(
            key_shared_mode=pulsar.KeySharedMode.Sticky,
            allow_out_of_order_delivery=True,
            sticky_ranges=[(0,30000)],
        )

        consumer = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.KeyShared, consumer_name='con-1',
                                    start_message_id_inclusive=True, key_shared_policy=consumer_key_shared_policy)

        consumer2_key_shared_policy = pulsar.ConsumerKeySharedPolicy(
            key_shared_mode=pulsar.KeySharedMode.Sticky,
            allow_out_of_order_delivery=True,
            sticky_ranges=[(30001, 65535)],
        )
        consumer2 = client.subscribe(topic, "my-sub", consumer_type=ConsumerType.KeyShared, consumer_name='con-2',
                                     start_message_id_inclusive=True, key_shared_policy=consumer2_key_shared_policy)
        producer = client.create_producer(topic)

        for i in range(10):
            if i > 0:
                time.sleep(0.02)
            producer.send(b"hello-%d" % i)

        msgs = []
        while True:
            try:
                msg = consumer.receive(100)
            except pulsar.Timeout:
                break
            msgs.append(msg)
            consumer.acknowledge(msg)

        while True:
            try:
                msg = consumer2.receive(100)
            except pulsar.Timeout:
                break
            msgs.append(msg)
            consumer2.acknowledge(msg)

        self.assertEqual(len(msgs), 10)
        client.close()

    def test_acknowledge_failed(self):
        client = Client(self.serviceUrl)
        topic = 'test_acknowledge_failed'
        producer = client.create_producer(topic)
        consumer1 = client.subscribe(topic, 'sub1', consumer_type=ConsumerType.Shared)
        consumer2 = client.subscribe(topic, 'sub2', consumer_type=ConsumerType.KeyShared)
        msg_id = producer.send('hello'.encode())
        msg1 = consumer1.receive()
        with self.assertRaises(pulsar.CumulativeAcknowledgementNotAllowedError):
            consumer1.acknowledge_cumulative(msg1)
        with self.assertRaises(pulsar.CumulativeAcknowledgementNotAllowedError):
            consumer1.acknowledge_cumulative(msg1.message_id())
        msg2 = consumer2.receive()
        with self.assertRaises(pulsar.CumulativeAcknowledgementNotAllowedError):
            consumer2.acknowledge_cumulative(msg2)
        with self.assertRaises(pulsar.CumulativeAcknowledgementNotAllowedError):
            consumer2.acknowledge_cumulative(msg2.message_id())
        consumer = client.subscribe([topic, topic + '-another'], 'sub')
        # The message id does not have a topic name
        with self.assertRaises(pulsar.OperationNotSupported):
            consumer.acknowledge(msg_id)
        client.close()

    def test_batch_index_ack(self):
        topic_name = 'test-batch-index-ack-3'
        client = pulsar.Client('pulsar://localhost:6650')
        producer = client.create_producer(topic_name,
                                          batching_enabled=True,
                                          batching_max_messages=100,
                                          batching_max_publish_delay_ms=10000)
        consumer = client.subscribe(topic_name,
                                    subscription_name='test-batch-index-ack',
                                    batch_index_ack_enabled=True)

        # Make sure send 0~5 is a batch msg.
        for i in range(5):
            producer.send_async(b"hello-%d" % i, callback=None)
        producer.flush()

        # Receive msgs and just ack 0, 1 msgs
        results = []
        for i in range(5):
            msg = consumer.receive()
            print("receive from {}".format(msg.message_id()))
            results.append(msg)
        assert len(results) == 5
        for i in range(2):
            consumer.acknowledge(results[i])
            time.sleep(0.2)

        # Restart consumer after, just receive 2~5 msg.
        consumer.close()
        consumer = client.subscribe(topic_name,
                                    subscription_name='test-batch-index-ack',
                                    batch_index_ack_enabled=True)
        results2 = []
        for i in range(2, 5):
            msg = consumer.receive()
            results2.append(msg)
        assert len(results2) == 3
        # assert no more msgs.
        with self.assertRaises(pulsar.Timeout):
            consumer.receive(timeout_millis=1000)

        client.close()


if __name__ == "__main__":
    main()
