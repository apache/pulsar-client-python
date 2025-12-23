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
from typing import Any, List, Union

import _pulsar
from _pulsar import InitialPosition
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

class Consumer:
    """
    The Pulsar message consumer, used to subscribe to messages from a topic.
    """

    def __init__(self, consumer: _pulsar.Consumer) -> None:
        """
        Create the consumer.
        Users should not call this constructor directly. Instead, create the
        consumer via `Client.subscribe`.

        Parameters
        ----------
        consumer: _pulsar.Consumer
            The underlying Consumer object from the C extension.
        """
        self._consumer: _pulsar.Consumer = consumer

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
        m._schema = pulsar.schema.BytesSchema()
        return m

    async def acknowledge(self, message: Union[pulsar.Message, pulsar.MessageId, _pulsar.Message, _pulsar.MessageId]) -> None:
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

    async def acknowledge_cumulative(self, message: Union[pulsar.Message, pulsar.MessageId, _pulsar.Message, _pulsar.MessageId]) -> None:
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
        self._consumer.acknowledge_cumulative_async(msg, functools.partial(_set_future, future, value=None))
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

    async def create_producer(self, topic: str) -> Producer:
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
        # TODO: add more configs
        self._client.create_producer_async(topic, conf, functools.partial(_set_future, future))
        return Producer(await future)

    async def subscribe(self, topic: Union[str, List[str]], subscription_name: str,
                        is_pattern_topic: bool = False,
                        consumer_type: pulsar.ConsumerType = pulsar.ConsumerType.Exclusive,
                        initial_position: InitialPosition = InitialPosition.Latest) -> Consumer:
        """
        Subscribe to the given topic and subscription combination.

        Parameters
        ----------
        topic: str, List[str], or regex pattern
            The name of the topic, list of topics or regex pattern.
        subscription_name: str
            The name of the subscription.
        is_pattern_topic: bool, default=False
            Whether `topic` is a regex pattern. This option takes no effect when `topic` is a list of topics.
        consumer_type: pulsar.ConsumerType, default=pulsar.ConsumerType.Exclusive
            Select the subscription type to be used when subscribing to the topic.
        initial_position: InitialPosition, default=InitialPosition.Latest
            Set the initial position of a consumer when subscribing to the topic.
            It could be either: ``InitialPosition.Earliest`` or ``InitialPosition.Latest``.

        Returns
        -------
        Consumer
            The consumer created

        Raises
        ------
        PulsarException
        """
        future = asyncio.get_running_loop().create_future()
        conf = _pulsar.ConsumerConfiguration()
        conf.consumer_type(consumer_type)
        conf.subscription_initial_position(initial_position)

        if isinstance(topic, str):
            if is_pattern_topic:
                self._client.subscribe_async_pattern(topic, subscription_name, conf, functools.partial(_set_future, future))
            else:
                self._client.subscribe_async(topic, subscription_name, conf, functools.partial(_set_future, future))
        elif isinstance(topic, list):
            self._client.subscribe_async_topics(topic, subscription_name, conf, functools.partial(_set_future, future))
        else:
            raise ValueError("Argument 'topic' is expected to be of a type between (str, list)")

        return Consumer(await future)

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

def _set_future(future: asyncio.Future, result: _pulsar.Result, value: Any):
    def complete():
        if result == _pulsar.Result.Ok:
            future.set_result(value)
        else:
            future.set_exception(PulsarException(result))
    future.get_loop().call_soon_threadsafe(complete)
