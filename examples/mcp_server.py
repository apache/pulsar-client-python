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

"""
MCP (Model Context Protocol) server for Apache Pulsar.

Exposes three tools that allow AI agents (Cursor, Claude Desktop, etc.)
to produce, consume, and inspect Pulsar topics via natural language.

Prerequisites:
    pip install "mcp[cli]" pulsar-client

Usage:
    PULSAR_BROKER_URL=pulsar://localhost:6650 python3 examples/mcp_server.py

Configure in Cursor (~/.cursor/mcp.json) or Claude Desktop:
    {
      "mcpServers": {
        "pulsar": {
          "command": "python3",
          "args": ["examples/mcp_server.py"],
          "env": {
            "PULSAR_BROKER_URL": "pulsar://localhost:6650",
            "PULSAR_TOKEN": ""
          }
        }
      }
    }
"""

import logging
import os
import sys
from datetime import timedelta
from typing import Optional

import pulsar
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("pulsar")

_broker_url = os.getenv("PULSAR_BROKER_URL", "pulsar://localhost:6650")
_token = os.getenv("PULSAR_TOKEN", "")

# The C++ client logs to stdout by default, which would corrupt the
# MCP stdio JSON-RPC stream.  Route logs to stderr instead.
_pulsar_logger = logging.getLogger("pulsar")
_pulsar_logger.setLevel(logging.INFO)
if not _pulsar_logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _pulsar_logger.addHandler(_handler)


def _make_client() -> pulsar.Client:
    if _token:
        return pulsar.Client(
            _broker_url,
            authentication=pulsar.AuthenticationToken(_token),
            logger=_pulsar_logger,
        )
    return pulsar.Client(_broker_url, logger=_pulsar_logger)


@mcp.tool()
def pulsar_publish(
    topic: str,
    payload: str,
    properties: Optional[dict] = None,
    partition_key: Optional[str] = None,
    delay_seconds: Optional[int] = None,
) -> dict:
    """
    Publish a message to a Pulsar topic.

    Args:
        topic: Full topic name, e.g. persistent://public/default/my-topic
        payload: Message body as a string
        properties: Optional key-value string properties
        partition_key: Optional routing key for partitioned topics
        delay_seconds: Deliver the message after this many seconds
    """
    client = _make_client()
    try:
        producer = client.create_producer(topic)
        msg_id = producer.send(
            payload.encode("utf-8"),
            properties=properties or {},
            partition_key=partition_key,
            deliver_after=(
                timedelta(seconds=delay_seconds)
                if delay_seconds and delay_seconds > 0
                else None
            ),
        )
        producer.close()
        return {"message_id": str(msg_id)}
    finally:
        client.close()


@mcp.tool()
def pulsar_consume(
    topic: str,
    subscription: str,
    subscription_type: str = "Shared",
    initial_position: str = "earliest",
    max_messages: int = 10,
    timeout_ms: int = 3000,
) -> dict:
    """
    Consume messages from a Pulsar topic, auto-acknowledging each one.

    Args:
        topic: Full topic name, e.g. persistent://public/default/my-topic
        subscription: Subscription name
        subscription_type: Exclusive | Shared | Failover | Key_Shared
        initial_position: Where a NEW subscription starts: "earliest" or "latest"
                          (ignored if the subscription already exists)
        max_messages: Maximum number of messages to return (default 10)
        timeout_ms: Milliseconds to wait for each message (default 3000)
    """
    sub_type_map = {
        "Exclusive": pulsar.ConsumerType.Exclusive,
        "Shared": pulsar.ConsumerType.Shared,
        "Failover": pulsar.ConsumerType.Failover,
        "Key_Shared": pulsar.ConsumerType.KeyShared,
    }
    consumer_type = sub_type_map.get(subscription_type, pulsar.ConsumerType.Shared)
    init_pos = (
        pulsar.InitialPosition.Earliest
        if initial_position == "earliest"
        else pulsar.InitialPosition.Latest
    )

    client = _make_client()
    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_type=consumer_type,
            initial_position=init_pos,
        )
        messages = []
        for _ in range(max_messages):
            try:
                msg = consumer.receive(timeout_millis=timeout_ms)
                messages.append(
                    {
                        "message_id": str(msg.message_id()),
                        "topic": msg.topic_name(),
                        "publish_timestamp": msg.publish_timestamp(),
                        "properties": msg.properties(),
                        "payload": msg.data().decode("utf-8", errors="replace"),
                    }
                )
                consumer.acknowledge(msg)
            except Exception:
                break
        consumer.close()
        return {"messages": messages, "count": len(messages)}
    finally:
        client.close()


@mcp.tool()
def pulsar_peek(
    topic: str,
    start_from: str = "earliest",
    max_messages: int = 10,
    timeout_ms: int = 2000,
) -> dict:
    """
    Read messages from a topic using a Reader (no subscription, no ack).
    Useful for inspecting topic contents without affecting consumer state.

    Args:
        topic: Full topic name, e.g. persistent://public/default/my-topic
        start_from: "earliest" or "latest" (default "earliest")
        max_messages: Maximum messages to return (default 10)
        timeout_ms: Milliseconds to wait for each message (default 2000)
    """
    start_id = (
        pulsar.MessageId.earliest
        if start_from == "earliest"
        else pulsar.MessageId.latest
    )

    client = _make_client()
    try:
        reader = client.create_reader(topic, start_id)
        messages = []
        for _ in range(max_messages):
            if not reader.has_message_available():
                break
            try:
                msg = reader.read_next(timeout_millis=timeout_ms)
                messages.append(
                    {
                        "message_id": str(msg.message_id()),
                        "topic": msg.topic_name(),
                        "publish_timestamp": msg.publish_timestamp(),
                        "properties": msg.properties(),
                        "payload": msg.data().decode("utf-8", errors="replace"),
                    }
                )
            except Exception:
                break
        reader.close()
        return {"messages": messages, "count": len(messages)}
    finally:
        client.close()


if __name__ == "__main__":
    mcp.run()
