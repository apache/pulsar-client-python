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

# pylint: disable=no-name-in-module,c-extension-no-member,protected-access

"""
The Pulsar Python client APIs that work with the asyncio module.
"""

import asyncio
import functools
from datetime import timedelta
from typing import Any, Callable, List, Union

import _pulsar
from _pulsar import (
    InitialPosition,
    CompressionType,
    PartitionsRoutingMode,
    BatchingType,
    ProducerAccessMode,
    RegexSubscriptionMode,
    ConsumerCryptoFailureAction,
)
import pulsar

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

    def __init__(self, producer: _pulsar.Producer, schema: pulsar.schema.Schema) -> None:
        """
        Create the producer.
        Users should not call this constructor directly. Instead, create the
        producer via `Client.create_producer`.

        Parameters
        ----------
        producer: _pulsar.Producer
            The underlying Producer object from the C extension.
        schema: pulsar.schema.Schema
            The schema of the data that will be sent by this producer.
        """
        self._producer = producer
        self._schema = schema

    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    async def send(self, content: Any,
                   properties: dict | None = None,
                   partition_key: str | None = None,
                   ordering_key: str | None = None,
                   sequence_id: int | None = None,
                   replication_clusters: List[str] | None = None,
                   disable_replication: bool | None = None,
                   event_timestamp: int | None = None,
                   deliver_at: int | None = None,
                   deliver_after: timedelta | None = None) -> pulsar.MessageId:
        """
        Send a message asynchronously.

        parameters
        ----------
        content: Any
            The message payload, whose type should respect the schema defined in
            `Client.create_producer`.
        properties: dict | None
            A dict of application-defined string properties.
        partition_key: str | None
            Sets the partition key for the message routing. A hash of this key is
            used to determine the message's topic partition.
        ordering_key: str | None
            Sets the ordering key for the message routing.
        sequence_id: int | None
            Specify a custom sequence id for the message being published.
        replication_clusters: List[str] | None
            Override namespace replication clusters. Note that it is the caller's responsibility
            to provide valid cluster names and that all clusters have been previously configured
            as topics. Given an empty list, the message will replicate per the namespace
            configuration.
        disable_replication: bool | None
            Do not replicate this message.
        event_timestamp: int | None
            Timestamp in millis of the timestamp of event creation
        deliver_at: int | None
            Specify the message should not be delivered earlier than the specified timestamp.
        deliver_after: timedelta | None
            Specify a delay in timedelta for the delivery of the messages.

        Returns
        -------
        pulsar.MessageId
            The message id that represents the persisted position of the message.

        Raises
        ------
        PulsarException
        """
        builder = _pulsar.MessageBuilder()
        builder.content(self._schema.encode(content))

        if properties is not None:
            for k, v in properties.items():
                builder.property(k, v)
        if partition_key is not None:
            builder.partition_key(partition_key)
        if ordering_key is not None:
            builder.ordering_key(ordering_key)
        if sequence_id is not None:
            builder.sequence_id(sequence_id)
        if replication_clusters is not None:
            builder.replication_clusters(replication_clusters)
        if disable_replication is not None:
            builder.disable_replication(disable_replication)
        if event_timestamp is not None:
            builder.event_timestamp(event_timestamp)
        if deliver_at is not None:
            builder.deliver_at(deliver_at)
        if deliver_after is not None:
            builder.deliver_after(deliver_after)

        future = asyncio.get_running_loop().create_future()
        self._producer.send_async(builder.build(), functools.partial(_set_future, future))
        msg_id = await future
        return pulsar.MessageId(
            msg_id.partition(),
            msg_id.ledger_id(),
            msg_id.entry_id(),
            msg_id.batch_index(),
        )

    async def flush(self) -> None:
        """
        Flush all the messages buffered in the producer asynchronously.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._producer.flush_async(functools.partial(_set_future, future, value=None))
        await future

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

    def topic(self):
        """
        Return the topic which producer is publishing to
        """
        return self._producer.topic()

    def producer_name(self):
        """
        Return the producer name which could have been assigned by the
        system or specified by the client
        """
        return self._producer.producer_name()

    def last_sequence_id(self):
        """
        Return the last sequence id that was published and acknowledged by this producer.

        The sequence id can be either automatically assigned or custom set on the message.
        After recreating a producer with the same name, this will return the sequence id
        of the last message that was published in the previous session, or -1 if no
        message was ever published.
        """
        return self._producer.last_sequence_id()

class Consumer:
    """
    The Pulsar message consumer, used to subscribe to messages from a topic.
    """

    def __init__(self, consumer: _pulsar.Consumer, schema: pulsar.schema.Schema) -> None:
        """
        Create the consumer.
        Users should not call this constructor directly. Instead, create the
        consumer via `Client.subscribe`.

        Parameters
        ----------
        consumer: _pulsar.Consumer
            The underlying Consumer object from the C extension.
        schema: pulsar.schema.Schema
            The schema of the data that will be received by this consumer.
        """
        self._consumer = consumer
        self._schema = schema

    async def receive(self) -> pulsar.Message:
        """
        Receive a single message asynchronously.

        Returns
        -------
        pulsar.Message
            The message received.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.receive_async(functools.partial(_set_future, future))
        msg = await future
        m = pulsar.Message()
        m._message = msg
        m._schema = self._schema
        return m

    async def acknowledge(
        self,
        message: Union[pulsar.Message, pulsar.MessageId,
                       _pulsar.Message, _pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the reception of a single message asynchronously.

        Parameters
        ----------
        message : Message, MessageId, _pulsar.Message, _pulsar.MessageId
            The received message or message id.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(message, pulsar.Message):
            msg = message._message
        elif isinstance(message, pulsar.MessageId):
            msg = message._msg_id
        else:
            msg = message
        self._consumer.acknowledge_async(msg, functools.partial(_set_future, future, value=None))
        await future

    async def acknowledge_cumulative(
        self,
        message: Union[pulsar.Message, pulsar.MessageId,
                       _pulsar.Message, _pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the reception of all the messages in the stream up to (and
        including) the provided message asynchronously.

        Parameters
        ----------
        message : Message, MessageId, _pulsar.Message, _pulsar.MessageId
            The received message or message id.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(message, pulsar.Message):
            msg = message._message
        elif isinstance(message, pulsar.MessageId):
            msg = message._msg_id
        else:
            msg = message
        self._consumer.acknowledge_cumulative_async(
            msg, functools.partial(_set_future, future, value=None)
        )
        await future

    async def unsubscribe(self) -> None:
        """
        Unsubscribe the current consumer from the topic asynchronously.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.unsubscribe_async(functools.partial(_set_future, future, value=None))
        await future

    async def seek(self, messageid: Union[pulsar.MessageId, int]) -> None:
        """
        Reset the subscription associated with this consumer to a specific
        message id or publish timestamp asynchronously.

        The message id can either be a specific message or represent the first
        or last messages in the topic.

        Parameters
        ----------
        messageid : MessageId or int
            The message id for seek, OR an integer event time (timestamp) to
            seek to

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        if isinstance(messageid, pulsar.MessageId):
            msg_id = messageid._msg_id
        elif isinstance(messageid, int):
            msg_id = messageid
        else:
            raise ValueError(f"invalid messageid type {type(messageid)}")
        self._consumer.seek_async(
            msg_id, functools.partial(_set_future, future, value=None)
        )
        await future

    async def close(self) -> None:
        """
        Close the consumer asynchronously.

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.close_async(functools.partial(_set_future, future, value=None))
        await future

    def topic(self) -> str:
        """
        Return the topic this consumer is subscribed to.
        """
        return self._consumer.topic()

    def subscription_name(self) -> str:
        """
        Return the subscription name.
        """
        return self._consumer.subscription_name()

    def consumer_name(self) -> str:
        """
        Return the consumer name.
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
        self._client: _pulsar.Client = pulsar.Client(service_url, **kwargs)._client

    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    async def create_producer(self, topic: str,
                              producer_name: str | None = None,
                              schema: pulsar.schema.Schema | None = None,
                              initial_sequence_id: int | None = None,
                              send_timeout_millis: int = 30000,
                              compression_type: CompressionType = CompressionType.NONE,
                              max_pending_messages: int = 1000,
                              max_pending_messages_across_partitions: int = 50000,
                              block_if_queue_full: bool = False,
                              batching_enabled: bool = True,
                              batching_max_messages: int = 1000,
                              batching_max_allowed_size_in_bytes: int = 128*1024,
                              batching_max_publish_delay_ms: int = 10,
                              chunking_enabled: bool = False,
                              message_routing_mode: PartitionsRoutingMode =
                              PartitionsRoutingMode.RoundRobinDistribution,
                              lazy_start_partitioned_producers: bool = False,
                              properties: dict | None = None,
                              batching_type: BatchingType = BatchingType.Default,
                              encryption_key: str | None = None,
                              crypto_key_reader: pulsar.CryptoKeyReader | None = None,
                              access_mode: ProducerAccessMode = ProducerAccessMode.Shared,
                              message_router: Callable[[pulsar.Message, int], int] | None = None,
                              ) -> Producer:
        """
        Create a new producer on a given topic

        Parameters
        ----------
        topic: str
            The topic name
        producer_name: str | None
            Specify a name for the producer. If not assigned, the system will generate a globally
            unique name which can be accessed with `Producer.producer_name()`. When specifying a
            name, it is up to the user to ensure that, for a given topic, the producer name is
            unique across all Pulsar's clusters.
        schema: pulsar.schema.Schema | None, default=None
            Define the schema of the data that will be published by this producer.
        initial_sequence_id: int | None, default=None
            Set the baseline for the sequence ids for messages published by
            the producer.
        send_timeout_millis: int, default=30000
            If a message is not acknowledged by the server before the
            send_timeout expires, an error will be reported.
        compression_type: CompressionType, default=CompressionType.NONE
            Set the compression type for the producer.
        max_pending_messages: int, default=1000
            Set the max size of the queue holding the messages pending to
            receive an acknowledgment from the broker.
        max_pending_messages_across_partitions: int, default=50000
            Set the max size of the queue holding the messages pending to
            receive an acknowledgment across partitions.
        block_if_queue_full: bool, default=False
            Set whether send operations should block when the outgoing
            message queue is full.
        batching_enabled: bool, default=True
            Enable automatic message batching. Note that, unlike the synchronous producer API in
            ``pulsar.Client.create_producer``, batching is enabled by default for the asyncio
            producer.
        batching_max_messages: int, default=1000
            Maximum number of messages in a batch.
        batching_max_allowed_size_in_bytes: int, default=128*1024
            Maximum size in bytes of a batch.
        batching_max_publish_delay_ms: int, default=10
            The batch interval in milliseconds.
        chunking_enabled: bool, default=False
            Enable chunking of large messages.
        message_routing_mode: PartitionsRoutingMode,
            default=PartitionsRoutingMode.RoundRobinDistribution
            Set the message routing mode for the partitioned producer.
        lazy_start_partitioned_producers: bool, default=False
            Start partitioned producers lazily on demand.
        properties: dict | None, default=None
            Sets the properties for the producer.
        batching_type: BatchingType, default=BatchingType.Default
            Sets the batching type for the producer.
        encryption_key: str | None, default=None
            The key used for symmetric encryption.
        crypto_key_reader: pulsar.CryptoKeyReader | None, default=None
            Symmetric encryption class implementation.
        access_mode: ProducerAccessMode, default=ProducerAccessMode.Shared
            Set the type of access mode that the producer requires on the topic.
        message_router: Callable[[pulsar.Message, int], int] | None, default=None
            A custom message router function that takes a Message and the
            number of partitions and returns the partition index.

        Returns
        -------
        Producer
            The producer created

        Raises
        ------
        PulsarException
        """
        if batching_enabled and chunking_enabled:
            raise ValueError("Batching and chunking of messages can't be enabled together.")

        if schema is None:
            schema = pulsar.schema.BytesSchema()
        schema.attach_client(self._client)

        future = asyncio.get_running_loop().create_future()
        conf = _pulsar.ProducerConfiguration()
        if producer_name is not None:
            conf.producer_name(producer_name)
        conf.schema(schema.schema_info())
        if initial_sequence_id is not None:
            conf.initial_sequence_id(initial_sequence_id)
        conf.send_timeout_millis(send_timeout_millis)
        conf.compression_type(compression_type)
        conf.max_pending_messages(max_pending_messages)
        conf.max_pending_messages_across_partitions(max_pending_messages_across_partitions)
        conf.block_if_queue_full(block_if_queue_full)
        conf.batching_enabled(batching_enabled)
        conf.batching_max_messages(batching_max_messages)
        conf.batching_max_allowed_size_in_bytes(batching_max_allowed_size_in_bytes)
        conf.batching_max_publish_delay_ms(batching_max_publish_delay_ms)
        conf.chunking_enabled(chunking_enabled)
        conf.partitions_routing_mode(message_routing_mode)
        conf.lazy_start_partitioned_producers(lazy_start_partitioned_producers)
        if properties is not None:
            for k, v in properties.items():
                conf.property(k, v)
        conf.batching_type(batching_type)
        if encryption_key is not None:
            conf.encryption_key(encryption_key)
        if crypto_key_reader is not None:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)
        conf.access_mode(access_mode)
        if message_router is not None:
            def underlying_router(msg: _pulsar.Message, num_partitions: int) -> int:
                return message_router(pulsar.Message._wrap(msg), num_partitions)
            conf.message_router(underlying_router)

        self._client.create_producer_async(
            topic, conf, functools.partial(_set_future, future)
        )
        return Producer(await future, schema)

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-positional-arguments
    async def subscribe(self, topic: Union[str, List[str]],
                        subscription_name: str,
                        consumer_type: pulsar.ConsumerType =
                        pulsar.ConsumerType.Exclusive,
                        schema: pulsar.schema.Schema | None = None,
                        receiver_queue_size: int = 1000,
                        max_total_receiver_queue_size_across_partitions: int =
                        50000,
                        consumer_name: str | None = None,
                        unacked_messages_timeout_ms: int | None = None,
                        broker_consumer_stats_cache_time_ms: int = 30000,
                        negative_ack_redelivery_delay_ms: int = 60000,
                        is_read_compacted: bool = False,
                        properties: dict | None = None,
                        initial_position: InitialPosition = InitialPosition.Latest,
                        crypto_key_reader: pulsar.CryptoKeyReader | None = None,
                        replicate_subscription_state_enabled: bool = False,
                        max_pending_chunked_message: int = 10,
                        auto_ack_oldest_chunked_message_on_queue_full: bool = False,
                        start_message_id_inclusive: bool = False,
                        batch_receive_policy: pulsar.ConsumerBatchReceivePolicy | None =
                        None,
                        key_shared_policy: pulsar.ConsumerKeySharedPolicy | None =
                        None,
                        batch_index_ack_enabled: bool = False,
                        regex_subscription_mode: RegexSubscriptionMode =
                        RegexSubscriptionMode.PersistentOnly,
                        dead_letter_policy: pulsar.ConsumerDeadLetterPolicy | None =
                        None,
                        crypto_failure_action: ConsumerCryptoFailureAction =
                        ConsumerCryptoFailureAction.FAIL,
                        is_pattern_topic: bool = False) -> Consumer:
        """
        Subscribe to the given topic and subscription combination.

        Parameters
        ----------
        topic: str, List[str], or regex pattern
            The name of the topic, list of topics or regex pattern.
            When `is_pattern_topic` is True, `topic` is treated as a regex.
        subscription_name: str
            The name of the subscription.
        consumer_type: pulsar.ConsumerType, default=pulsar.ConsumerType.Exclusive
            Select the subscription type to be used when subscribing to the topic.
        schema: pulsar.schema.Schema | None, default=None
            Define the schema of the data that will be received by this consumer.
        receiver_queue_size: int, default=1000
            Sets the size of the consumer receive queue.
        max_total_receiver_queue_size_across_partitions: int, default=50000
            Set the max total receiver queue size across partitions.
        consumer_name: str | None, default=None
            Sets the consumer name.
        unacked_messages_timeout_ms: int | None, default=None
            Sets the timeout in milliseconds for unacknowledged messages.
        broker_consumer_stats_cache_time_ms: int, default=30000
            Sets the time duration for which the broker-side consumer stats
            will be cached in the client.
        negative_ack_redelivery_delay_ms: int, default=60000
            The delay after which to redeliver the messages that failed to be
            processed.
        is_read_compacted: bool, default=False
            Selects whether to read the compacted version of the topic.
        properties: dict | None, default=None
            Sets the properties for the consumer.
        initial_position: InitialPosition, default=InitialPosition.Latest
            Set the initial position of a consumer when subscribing to the topic.
        crypto_key_reader: pulsar.CryptoKeyReader | None, default=None
            Symmetric encryption class implementation.
        replicate_subscription_state_enabled: bool, default=False
            Set whether the subscription status should be replicated.
        max_pending_chunked_message: int, default=10
            Consumer buffers chunk messages into memory until it receives all the chunks.
        auto_ack_oldest_chunked_message_on_queue_full: bool, default=False
            Automatically acknowledge oldest chunked messages on queue
            full.
        start_message_id_inclusive: bool, default=False
            Set the consumer to include the given position of any reset
            operation.
        batch_receive_policy: pulsar.ConsumerBatchReceivePolicy | None, default=None
            Set the batch collection policy for batch receiving.
        key_shared_policy: pulsar.ConsumerKeySharedPolicy | None, default=None
            Set the key shared policy for use when the ConsumerType is
            KeyShared.
        batch_index_ack_enabled: bool, default=False
            Enable the batch index acknowledgement.
        regex_subscription_mode: RegexSubscriptionMode,
            default=RegexSubscriptionMode.PersistentOnly
            Set the regex subscription mode for use when the topic is a regex
            pattern.
        dead_letter_policy: pulsar.ConsumerDeadLetterPolicy | None, default=None
            Set dead letter policy for consumer.
        crypto_failure_action: ConsumerCryptoFailureAction,
            default=ConsumerCryptoFailureAction.FAIL
            Set the behavior when the decryption fails.
        is_pattern_topic: bool, default=False
            Whether `topic` is a regex pattern. If it's True when `topic` is a list, a ValueError
            will be raised.

        Returns
        -------
        Consumer
            The consumer created

        Raises
        ------
        PulsarException
        """
        if schema is None:
            schema = pulsar.schema.BytesSchema()

        future = asyncio.get_running_loop().create_future()
        conf = _pulsar.ConsumerConfiguration()
        conf.consumer_type(consumer_type)
        conf.regex_subscription_mode(regex_subscription_mode)
        conf.read_compacted(is_read_compacted)
        conf.receiver_queue_size(receiver_queue_size)
        conf.max_total_receiver_queue_size_across_partitions(
            max_total_receiver_queue_size_across_partitions
        )
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
        conf.auto_ack_oldest_chunked_message_on_queue_full(
            auto_ack_oldest_chunked_message_on_queue_full
        )
        conf.start_message_id_inclusive(start_message_id_inclusive)
        if batch_receive_policy:
            conf.batch_receive_policy(batch_receive_policy.policy())

        if key_shared_policy:
            conf.key_shared_policy(key_shared_policy.policy())
        conf.batch_index_ack_enabled(batch_index_ack_enabled)
        if dead_letter_policy:
            conf.dead_letter_policy(dead_letter_policy.policy())
        conf.crypto_failure_action(crypto_failure_action)

        if isinstance(topic, str):
            if is_pattern_topic:
                self._client.subscribe_async_pattern(
                    topic, subscription_name, conf,
                    functools.partial(_set_future, future)
                )
            else:
                self._client.subscribe_async(
                    topic, subscription_name, conf,
                    functools.partial(_set_future, future)
                )
        elif isinstance(topic, list):
            if is_pattern_topic:
                raise ValueError(
                    "Argument 'topic' must be a string when "
                    "'is_pattern_topic' is True; lists of topics do not "
                    "support pattern subscriptions"
                )
            self._client.subscribe_async_topics(
                topic, subscription_name, conf,
                functools.partial(_set_future, future)
            )
        else:
            raise ValueError( "Argument 'topic' is expected to be of type 'str' or 'list'")

        schema.attach_client(self._client)
        return Consumer(await future, schema)

    async def close(self) -> None:
        """
        Close the client and all the associated producers and consumers

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        self._client.close_async(
            functools.partial(_set_future, future, value=None)
        )
        await future

def _set_future(future: asyncio.Future, result: _pulsar.Result, value: Any):
    def complete():
        if result == _pulsar.Result.Ok:
            future.set_result(value)
        else:
            future.set_exception(PulsarException(result))
    future.get_loop().call_soon_threadsafe(complete)
