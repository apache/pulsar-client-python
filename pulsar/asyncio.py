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
The Pulsar Python client APIs that work with the asyncio module.
"""

import asyncio
import functools
import logging
from typing import Any, Iterable, Optional, Union, Tuple

import _pulsar

import pulsar
from pulsar import Message, _listener_wrapper

from pulsar import schema
_schema = schema

class PulsarException(BaseException):
    """
    The exception that wraps the Pulsar error code
    """

    def __init__(self, result: pulsar.Result) -> None:
        """
        Create the Pulsar exception.

        Parameters
        ----------
        result: pulsar.Result
            The error code of the underlying Pulsar APIs.
        """
        self._result = result

    def error(self) -> pulsar.Result:
        """
        Returns the Pulsar error code.
        """
        return self._result

    def __str__(self):
        """
        Convert the exception to string.
        """
        return f'{self._result.value} {self._result.name}'


class Producer:
    """
    The Pulsar message producer, used to publish messages on a topic.
    """

    def __init__(self, producer: _pulsar.Producer) -> None:
        """
        Create the producer.
        Users should not call this constructor directly. Instead, create the
        producer via `Client.create_producer`.

        Parameters
        ----------
        producer: _pulsar.Producer
            The underlying Producer object from the C extension.
        """
        self._producer: _pulsar.Producer = producer

    async def send(self, content: bytes) -> pulsar.MessageId:
        """
        Send a message asynchronously.

        parameters
        ----------
        content: bytes
            The message payload

        Returns
        -------
        pulsar.MessageId
            The message id that represents the persisted position of the message.

        Raises
        ------
        PulsarException
        """
        builder = _pulsar.MessageBuilder()
        builder.content(content)
        future = asyncio.get_running_loop().create_future()
        self._producer.send_async(builder.build(), functools.partial(_set_future, future))
        msg_id = await future
        return pulsar.MessageId(
            msg_id.partition(),
            msg_id.ledger_id(),
            msg_id.entry_id(),
            msg_id.batch_index(),
        )

    async def close(self) -> None:
        """
        Close the producer.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._producer.close_async(functools.partial(_set_future, future, value=None))
        await future

    async def flush(self):
        """
        Flush all the messages buffered in the client and wait until all messages have been successfully persisted.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._producer.flush_async(functools.partial(_set_future, future, value=None))
        await future

    @property
    def is_connected(self) -> bool:
        """
        Check if the producer is connected or not.
        """

        return self._producer.is_connected()

    @property
    def last_sequence_id(self) -> int:
        """
        Get the last sequence id
        """
        return self._producer.last_sequence_id()

    @property
    def name(self) -> str:
        """
        Get the name of the producer.
        """
        return self._producer.producer_name()

    @property
    def topic(self) -> str:
        """
        Get the topic name of the producer.
        """
        return self._producer.topic()


class Consumer:
    # BUG: schema stuff doesn´t work at all because 90% of the methods are missing
    def __init__(self, consumer: _pulsar.Consumer):
        self._consumer: _pulsar.Consumer = consumer

    def _prepare_logger(logger):
        import logging
        def log(level, message):
            old_threads = logging.logThreads
            logging.logThreads = False
            logger.log(logging.getLevelName(level), message)
            logging.logThreads = old_threads
        return log

    async def acknowledge(self, msg: pulsar.Message) -> None:
        """
        Acknowledge the reception of a single message.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.acknowledge_async(msg._message, functools.partial(_set_future, future))
        await future

    async def acknowledge_cumulative(self, msg: pulsar.Message) -> None:
        """
        Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.acknowledge_cumulative_async(msg, functools.partial(_set_future, future))
        await future

    async def negative_acknowledge(self, msg: pulsar.Message) -> None:
        """
        Acknowledge the failure to process a single message.
        """
        self._consumer.negative_acknowledge(msg._message)

    async def batch_receive(self) -> Iterable[pulsar.Message]:
        """
        Batch receiving messages.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.batch_receive_async(functools.partial(_set_future, future))
        return await future

    async def receive(self) -> pulsar.Message:
        """
        Receive a single message.
        """
        future = asyncio.get_running_loop().create_future()

        self._consumer.receive_async(functools.partial(_set_future, future))
        msg = await future

        m = Message()
        m._message = msg
        m._schema = self._schema
        return m

    async def close(self):
        """
        Close the consumer.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.close_async(functools.partial(_set_future, future, value=None))
        await future

    async def seek(self, position: Union[Tuple[int, int, int, int], pulsar.MessageId]):
        """
        Reset the subscription associated with this consumer to a specific message id or publish timestamp. The message id can either be a specific message or represent the first or last messages in the topic. ...
        """
        if isinstance(position, tuple):
            partition, ledger_id, entry_id, batch_index = position
            message_id = _pulsar.MessageId(partition, ledger_id, entry_id, batch_index)
        else:
            message_id = position

        future = asyncio.get_running_loop().create_future()
        self._consumer.seek_async(message_id, functools.partial(_set_future, future))
        await future

    async def unsubscribe(self):
        """
        Unsubscribe the current consumer from the topic.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.unsubscribe_async(functools.partial(_set_future, future))
        await future

    def pause_message_listener(self):
        """
        Pause receiving messages via the message_listener until resume_message_listener() is called.
        """
        self._consumer.pause_message_listener()

    def resume_message_listener(self):
        """
        Resume receiving the messages via the message listener. Asynchronously receive all the messages enqueued from the time pause_message_listener() was called.
        """
        self._consumer.resume_message_listener()

    def redeliver_unacknowledged_messages(self):
        """
        Redelivers all the unacknowledged messages. In failover mode, the request is ignored if the consumer is not active for the given topic. In shared mode, the consumer's messages to be redelivered are distributed across all the connected consumers...
        """
        self._consumer.redeliver_unacknowledged_messages()

    @property
    def last_message_id(self) -> pulsar.MessageId:
        """
        MessageId of the last consumed message
        """
        return self._consumer.get_last_message_id()

    @property
    def is_connected(self) -> bool:
        """
        True if the consumer is connected to a broker
        """
        return self._consumer.is_connected()

    @property
    def subscription_name(self) -> str:
        """
        Name of the current subscription
        """
        return self._consumer.subscription_name()

    @property
    def topic(self) -> str:
        """
        Topic(s) of consumer
        """
        return self._consumer.topic()

    @property
    def consumer_name(self) -> str:
        """
        Name of consumer
        """
        return self._consumer.consumer_name()


class Client:
    """
    The asynchronous version of `pulsar.Client`.
    """

    def __init__(self, service_url, **kwargs) -> None:
        """
        See `pulsar.Client.__init__`
        """
        assert service_url.startswith('pulsar://'), "The service url must start with 'pulsar://'"
        self._client = pulsar.Client(service_url, **kwargs)._client
        self._consumers = []

    async def subscribe(self, topic, subscription_name,
                        consumer_type: _pulsar.ConsumerType = _pulsar.ConsumerType.Exclusive,
                        schema=pulsar.schema.BytesSchema(),
                        message_listener=None,
                        receiver_queue_size=1000,
                        max_total_receiver_queue_size_across_partitions=50000,
                        consumer_name=None,
                        unacked_messages_timeout_ms=None,
                        broker_consumer_stats_cache_time_ms=30000,
                        negative_ack_redelivery_delay_ms=60000,
                        is_read_compacted=False,
                        properties=None,
                        pattern_auto_discovery_period=60,
                        initial_position: _pulsar.InitialPosition = _pulsar.InitialPosition.Latest,
                        crypto_key_reader: Union[None, _pulsar.CryptoKeyReader] = None,
                        replicate_subscription_state_enabled=False,
                        max_pending_chunked_message=10,
                        auto_ack_oldest_chunked_message_on_queue_full=False,
                        start_message_id_inclusive=False,
                        batch_receive_policy=None,
                        key_shared_policy=None,
                        batch_index_ack_enabled=False,
                        regex_subscription_mode: _pulsar.RegexSubscriptionMode = _pulsar.RegexSubscriptionMode.PersistentOnly,
                        dead_letter_policy: Union[None, pulsar.ConsumerDeadLetterPolicy] = None,) -> Consumer:
        conf = _pulsar.ConsumerConfiguration()
        conf.consumer_type(consumer_type)
        conf.regex_subscription_mode(regex_subscription_mode)
        conf.read_compacted(is_read_compacted)


        if message_listener:
            conf.message_listener(_listener_wrapper(message_listener, schema))
        conf.receiver_queue_size(receiver_queue_size)
        conf.max_total_receiver_queue_size_across_partitions(max_total_receiver_queue_size_across_partitions)
        if consumer_name:
            conf.consumer_name(consumer_name)
        if unacked_messages_timeout_ms:
            conf.unacked_messages_timeout_ms(unacked_messages_timeout_ms)

        conf.negative_ack_redelivery_delay_ms(negative_ack_redelivery_delay_ms)
        conf.broker_consumer_stats_cache_time_ms(broker_consumer_stats_cache_time_ms)
        if properties:
            for k, v in properties.items():
                conf.property(k, v)
        conf.subscription_initial_position(initial_position)

        conf.schema(schema.schema_info())

        if crypto_key_reader:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)

        conf.replicate_subscription_state_enabled(replicate_subscription_state_enabled)
        conf.max_pending_chunked_message(max_pending_chunked_message)
        conf.auto_ack_oldest_chunked_message_on_queue_full(auto_ack_oldest_chunked_message_on_queue_full)
        conf.start_message_id_inclusive(start_message_id_inclusive)
        if batch_receive_policy:
            conf.batch_receive_policy(batch_receive_policy.policy())

        if key_shared_policy:
            conf.key_shared_policy(key_shared_policy.policy())
        conf.batch_index_ack_enabled(batch_index_ack_enabled)
        if dead_letter_policy:
            conf.dead_letter_policy(dead_letter_policy.policy())

        future = asyncio.get_running_loop().create_future()

        c = Consumer(None)
        if isinstance(topic, str):
            self._client.subscribe_async(topic, subscription_name, conf, functools.partial(_set_future, future))
            c._consumer = await future
        elif isinstance(topic, list):
            self._client.subscribe_topics_async(topic, subscription_name, conf, functools.partial(_set_future, future))
            c._consumer = await future
        elif isinstance(topic, pulsar._retype):
            self._client.subscribe_pattern_async(topic, subscription_name, conf, functools.partial(_set_future, future))
            c._consumer = await future
        else:
            raise ValueError("Argument 'topic' is expected to be of a type between (str, list, re.pattern)")

        c._client = self
        c._schema = schema
        c._schema.attach_client(self._client)

        self._consumers.append(c)

        return c

    async def create_producer(self, topic,
                              producer_name=None,
                              schema=pulsar.schema.BytesSchema(),
                              initial_sequence_id=None,
                              send_timeout_millis=30000,
                              compression_type: _pulsar.CompressionType = _pulsar.CompressionType.NONE,
                              max_pending_messages=1000,
                              max_pending_messages_across_partitions=50000,
                              block_if_queue_full=False,
                              batching_enabled=False,
                              batching_max_messages=1000,
                              batching_max_allowed_size_in_bytes=128*1024,
                              batching_max_publish_delay_ms=10,
                              chunking_enabled=False,
                              message_routing_mode: _pulsar.PartitionsRoutingMode = _pulsar.PartitionsRoutingMode.RoundRobinDistribution,
                              lazy_start_partitioned_producers=False,
                              properties=None,
                              batching_type: _pulsar.BatchingType = _pulsar.BatchingType.Default,
                              encryption_key=None,
                              crypto_key_reader: Union[None, _pulsar.CryptoKeyReader] = None,
                              access_mode: _pulsar.ProducerAccessMode = _pulsar.ProducerAccessMode.Shared,

                              ) -> Producer:
        """
        Create a new producer on a given topic

        Parameters
        ----------
        topic: str
            The topic name

        Returns
        -------
        Producer
            The producer created

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()

        conf = _pulsar.ProducerConfiguration()
        conf.send_timeout_millis(send_timeout_millis)
        conf.compression_type(compression_type)
        conf.max_pending_messages(max_pending_messages)
        conf.max_pending_messages_across_partitions(max_pending_messages_across_partitions)
        conf.block_if_queue_full(block_if_queue_full)
        conf.batching_enabled(batching_enabled)
        conf.batching_max_messages(batching_max_messages)
        conf.batching_max_allowed_size_in_bytes(batching_max_allowed_size_in_bytes)
        conf.batching_max_publish_delay_ms(batching_max_publish_delay_ms)
        conf.partitions_routing_mode(message_routing_mode)
        conf.batching_type(batching_type)
        conf.chunking_enabled(chunking_enabled)
        conf.lazy_start_partitioned_producers(lazy_start_partitioned_producers)
        conf.access_mode(access_mode)
        if producer_name:
            conf.producer_name(producer_name)
        if initial_sequence_id:
            conf.initial_sequence_id(initial_sequence_id)
        if properties:
            for k, v in properties.items():
                conf.property(k, v)

        conf.schema(schema.schema_info())
        if encryption_key:
            conf.encryption_key(encryption_key)
        if crypto_key_reader:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)

        if batching_enabled and chunking_enabled:
            raise ValueError("Batching and chunking can´t be enabled at the same time")

        self._client.create_producer_async(topic, conf, functools.partial(_set_future, future))
        return Producer(await future)

    async def close(self) -> None:
        """
        Close the client and all the associated producers and consumers

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._client.close_async(functools.partial(_set_future, future, value=None))
        await future

    async def get_topic_partitions(self, topic: str):
        future = asyncio.get_running_loop().create_future()
        self._client.get_partitions_for_topic_async(topic, functools.partial(_set_future, future))
        return await future

    def shutdown(self) -> None:
        self._client.shutdown()


def _set_future(future: asyncio.Future, result: _pulsar.Result, value: Optional[Any] = None):
    def complete():
        if result == _pulsar.Result.Ok:
            future.set_result(value)
        else:
            future.set_exception(PulsarException(result))

    future.get_loop().call_soon_threadsafe(complete)

