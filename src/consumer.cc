/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "utils.h"

#include <pulsar/Consumer.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

void Consumer_unsubscribe(Consumer& consumer) {
    waitForAsyncResult([&consumer](ResultCallback callback) { consumer.unsubscribeAsync(callback); });
}

Message Consumer_receive(Consumer& consumer) {
    return waitForAsyncValue<Message>([&](ReceiveCallback callback) { consumer.receiveAsync(callback); });
}

Message Consumer_receive_timeout(Consumer& consumer, int timeoutMs) {
    Message msg;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = consumer.receive(msg, timeoutMs);
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
    return msg;
}

Messages Consumer_batch_receive(Consumer& consumer) {
    Messages msgs;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = consumer.batchReceive(msgs);
    Py_END_ALLOW_THREADS CHECK_RESULT(res);
    return msgs;
}

void Consumer_acknowledge(Consumer& consumer, const Message& msg) {
    waitForAsyncResult([&](ResultCallback callback) { consumer.acknowledgeAsync(msg, callback); });
}

void Consumer_acknowledge_message_id(Consumer& consumer, const MessageId& msgId) {
    waitForAsyncResult([&](ResultCallback callback) { consumer.acknowledgeAsync(msgId, callback); });
}

void Consumer_negative_acknowledge(Consumer& consumer, const Message& msg) {
    Py_BEGIN_ALLOW_THREADS consumer.negativeAcknowledge(msg);
    Py_END_ALLOW_THREADS
}

void Consumer_negative_acknowledge_message_id(Consumer& consumer, const MessageId& msgId) {
    Py_BEGIN_ALLOW_THREADS consumer.negativeAcknowledge(msgId);
    Py_END_ALLOW_THREADS
}

void Consumer_acknowledge_cumulative(Consumer& consumer, const Message& msg) {
    waitForAsyncResult([&](ResultCallback callback) { consumer.acknowledgeCumulativeAsync(msg, callback); });
}

void Consumer_acknowledge_cumulative_message_id(Consumer& consumer, const MessageId& msgId) {
    waitForAsyncResult(
        [&](ResultCallback callback) { consumer.acknowledgeCumulativeAsync(msgId, callback); });
}

void Consumer_close(Consumer& consumer) {
    waitForAsyncResult([&consumer](ResultCallback callback) { consumer.closeAsync(callback); });
}

void Consumer_pauseMessageListener(Consumer& consumer) { CHECK_RESULT(consumer.pauseMessageListener()); }

void Consumer_resumeMessageListener(Consumer& consumer) { CHECK_RESULT(consumer.resumeMessageListener()); }

void Consumer_seek(Consumer& consumer, const MessageId& msgId) {
    waitForAsyncResult([msgId, &consumer](ResultCallback callback) { consumer.seekAsync(msgId, callback); });
}

MessageId Consumer_get_last_message_id(Consumer& consumer) {
    MessageId msgId;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = consumer.getLastMessageId(msgId);
    Py_END_ALLOW_THREADS CHECK_RESULT(res);
    return msgId;
}

void Consumer_seek_timestamp(Consumer& consumer, uint64_t timestamp) {
    waitForAsyncResult(
        [timestamp, &consumer](ResultCallback callback) { consumer.seekAsync(timestamp, callback); });
}

bool Consumer_is_connected(Consumer& consumer) { return consumer.isConnected(); }

void Consumer_receiveAsync(Consumer& consumer, ReceiveCallback callback) {
    py::gil_scoped_release release;
    consumer.receiveAsync(callback);
}

void Consumer_acknowledgeAsync(Consumer& consumer, const Message& msg, ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.acknowledgeAsync(msg, callback);
}

void Consumer_acknowledgeAsync_message_id(Consumer& consumer, const MessageId& msgId,
                                          ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.acknowledgeAsync(msgId, callback);
}

void Consumer_acknowledgeCumulativeAsync(Consumer& consumer, const Message& msg, ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.acknowledgeCumulativeAsync(msg, callback);
}

void Consumer_acknowledgeCumulativeAsync_message_id(Consumer& consumer, const MessageId& msgId,
                                                    ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.acknowledgeCumulativeAsync(msgId, callback);
}

void Consumer_closeAsync(Consumer& consumer, ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.closeAsync(callback);
}

void Consumer_unsubscribeAsync(Consumer& consumer, ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.unsubscribeAsync(callback);
}

void Consumer_seekAsync(Consumer& consumer, const MessageId& msgId, ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.seekAsync(msgId, callback);
}

void Consumer_seekAsync_timestamp(Consumer& consumer, uint64_t timestamp, ResultCallback callback) {
    py::gil_scoped_release release;
    consumer.seekAsync(timestamp, callback);
}

void export_consumer(py::module_& m) {
    py::class_<Consumer>(m, "Consumer")
        .def(py::init<>())
        .def("topic", &Consumer::getTopic, "return the topic this consumer is subscribed to",
             py::return_value_policy::copy)
        .def("subscription_name", &Consumer::getSubscriptionName, py::return_value_policy::copy)
        .def("consumer_name", &Consumer::getConsumerName, py::return_value_policy::copy)
        .def("unsubscribe", &Consumer_unsubscribe)
        .def("receive", &Consumer_receive)
        .def("receive", &Consumer_receive_timeout)
        .def("batch_receive", &Consumer_batch_receive)
        .def("acknowledge", &Consumer_acknowledge)
        .def("acknowledge", &Consumer_acknowledge_message_id)
        .def("acknowledge_cumulative", &Consumer_acknowledge_cumulative)
        .def("acknowledge_cumulative", &Consumer_acknowledge_cumulative_message_id)
        .def("negative_acknowledge", &Consumer_negative_acknowledge)
        .def("negative_acknowledge", &Consumer_negative_acknowledge_message_id)
        .def("close", &Consumer_close)
        .def("pause_message_listener", &Consumer_pauseMessageListener)
        .def("resume_message_listener", &Consumer_resumeMessageListener)
        .def("redeliver_unacknowledged_messages", &Consumer::redeliverUnacknowledgedMessages)
        .def("seek", &Consumer_seek)
        .def("seek", &Consumer_seek_timestamp)
        .def("is_connected", &Consumer_is_connected)
        .def("get_last_message_id", &Consumer_get_last_message_id)
        .def("receive_async", &Consumer_receiveAsync)
        .def("acknowledge_async", &Consumer_acknowledgeAsync)
        .def("acknowledge_async", &Consumer_acknowledgeAsync_message_id)
        .def("acknowledge_cumulative_async", &Consumer_acknowledgeCumulativeAsync)
        .def("acknowledge_cumulative_async", &Consumer_acknowledgeCumulativeAsync_message_id)
        .def("close_async", &Consumer_closeAsync)
        .def("unsubscribe_async", &Consumer_unsubscribeAsync)
        .def("seek_async", &Consumer_seekAsync)
        .def("seek_async", &Consumer_seekAsync_timestamp);
}
