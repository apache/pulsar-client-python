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
from typing import Any, Iterable, Optional

import _pulsar

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
    def __init__(self, consumer: _pulsar.Consumer) -> None:
        self._consumer: _pulsar.Consumer = consumer

    async def acknowledge(self, msg: pulsar.Message) -> None:
        """
        Acknowledge the reception of a single message.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.acknowledge_async(msg, functools.partial(_set_future, future))
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
        self._consumer.negative_acknowledge(msg)

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
        return await future

    async def close(self):
        """
        Close the consumer.
        """
        future = asyncio.get_running_loop().create_future()
        self._consumer.close_async(functools.partial(_set_future, future, value=None))
        await future

    async def seek(self, position: tuple[int, int, int, int]):
        """
        Reset the subscription associated with this consumer to a specific message id or publish timestamp. The message id can either be a specific message or represent the first or last messages in the topic. ...
        """
        partition, ledger_id, entry_id, batch_index = position
        message_id = _pulsar.MessageId(partition, ledger_id, entry_id, batch_index)
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
    def last_message_id(self) -> int:
        return self._consumer.last_message_id

    @property
    def is_connected(self) -> bool:
        return self._consumer.is_connected()

    @property
    def subscription_name(self) -> str:
        return self._consumer.subscription_name()

    @property
    def topic(self) -> str:
        return self._consumer.topic()


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

    async def subscribe(self, topics: str, subscription_name: str, consumer_type: _pulsar.ConsumerType,
                        schema: Optional[_pulsar.SchemaInfo] = _pulsar.SchemaInfo(_pulsar.SchemaType.BYTES, "bytes", "")) -> Consumer:
        conf = _pulsar.ConsumerConfiguration()
        conf.consumer_type(consumer_type)
        conf.schema(schema)

        future = asyncio.get_running_loop().create_future()
        self._client.subscribe_async(topics, subscription_name, conf, functools.partial(_set_future, future))
        return Consumer(await future)

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
