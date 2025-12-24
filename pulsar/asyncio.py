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
from typing import Any, List, Union

import _pulsar
from _pulsar import (
    InitialPosition,
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

    async def send(self, content: Any) -> pulsar.MessageId:
        """
        Send a message asynchronously.

        parameters
        ----------
        content: Any
            The message payload, whose type should respect the schema defined in
            `Client.create_producer`.

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
                              schema: pulsar.schema.Schema | None = None,
                              ) -> Producer:
        """
        Create a new producer on a given topic

        Parameters
        ----------
        topic: str
            The topic name
        schema: pulsar.schema.Schema | None, default=None
            Define the schema of the data that will be published by this producer.

        Returns
        -------
        Producer
            The producer created

        Raises
        ------
        PulsarException
        """
        if schema is None:
            schema = pulsar.schema.BytesSchema()
        schema.attach_client(self._client)

        future = asyncio.get_running_loop().create_future()
        conf = _pulsar.ProducerConfiguration()
        conf.schema(schema.schema_info())

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
