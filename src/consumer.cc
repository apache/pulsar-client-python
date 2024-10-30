/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except
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
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/Result.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <memory>

namespace py = pybind11;

void Consumer_unsubscribe(Consumer& consumer) {
    waitForAsyncResult([&consumer](ResultCallback callback) { consumer.unsubscribeAsync(callback); });
}

void Consumer_unsubscribeAsync(Consumer& consumer, ResultCallback callback) {
    consumer.unsubscribeAsync([callback] (Result result) {
        py::gil_scoped_acquire acquire;
        callback(result);
    });
}

Message Consumer_receive(Consumer& consumer) {
    return waitForAsyncValue<Message>([&](ReceiveCallback callback) { consumer.receiveAsync(callback); });
}

void Consumer_receiveAsync(Consumer& consumer, ReceiveCallback callback) {
      py::gil_scoped_acquire acquire;
      consumer.receiveAsync(callback);
}

Message Consumer_receive_timeout(Consumer& consumer, int timeoutMs) {
    Message msg;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = consumer.receive(msg, timeoutMs);
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
    return msg;
}

// TODO: implement async variant
Messages Consumer_batch_receive(Consumer& consumer) {
    Messages msgs;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = consumer.batchReceive(msgs);
    Py_END_ALLOW_THREADS CHECK_RESULT(res);
    return msgs;
}

void Consumer_batch_receive_async(Consumer& consumer, BatchReceiveCallback callback){
    consumer.batchReceiveAsync([callback](pulsar::Result result, pulsar::Messages messages){
        py::gil_scoped_acquire acquire;
        callback(result, messages);
    });
}

void Consumer_acknowledge(Consumer& consumer, const Message& msg) {
    waitForAsyncResult([&](ResultCallback callback) { consumer.acknowledgeAsync(msg, callback); });
}

void Consumer_acknowledgeAsync(Consumer& consumer, const Message& msg, py::object callback){
    auto py_callback = std::make_shared<py::object>(callback);

    consumer.acknowledgeAsync(msg, [py_callback](pulsar::Result result){
        py::gil_scoped_acquire acquire;
        (*py_callback)(result, py::none());
    });
}

void Consumer_acknowledge_message_id(Consumer& consumer, const MessageId& msgId) {
    waitForAsyncResult([&](ResultCallback callback) { consumer.acknowledgeAsync(msgId, callback); });
}

void Consumer_acknowledge_message_id_Async(Consumer& consumer, const MessageId& msgId, py::object callback){
    auto py_callback = std::make_shared<py::object>(callback);

    consumer.acknowledgeAsync(msgId, [py_callback](pulsar::Result result){
        py::gil_scoped_acquire acquire;
        (*py_callback)(result, py::none());
    });
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

void Consumer_acknowledge_cumulativeAsync(Consumer& consumer, const Message& msg, py::object callback){
    auto py_callback = std::make_shared<py::object>(callback);

    consumer.acknowledgeCumulativeAsync(msg, [py_callback](pulsar::Result result){
        py::gil_scoped_acquire acquire;
        (*py_callback)(result);
    });
}

// TODO: implement async variant
void Consumer_acknowledge_cumulative_message_id(Consumer& consumer, const MessageId& msgId) {
    waitForAsyncResult(
        [&](ResultCallback callback) { consumer.acknowledgeCumulativeAsync(msgId, callback); });
}

void Consumer_close(Consumer& consumer) {
    waitForAsyncResult([&consumer](ResultCallback callback) { consumer.closeAsync(callback); });
}

void Consumer_closeAsync(Consumer& consumer, ResultCallback callback){
    py::gil_scoped_acquire acquire;
    consumer.closeAsync(callback);
}

void Consumer_pauseMessageListener(Consumer& consumer) { CHECK_RESULT(consumer.pauseMessageListener()); }

void Consumer_resumeMessageListener(Consumer& consumer) { CHECK_RESULT(consumer.resumeMessageListener()); }

// TODO: implement async variant
void Consumer_seek(Consumer& consumer, const MessageId& msgId) {
    waitForAsyncResult([msgId, &consumer](ResultCallback callback) { consumer.seekAsync(msgId, callback); });
}

void Consumer_seekAsync(Consumer& consumer, const MessageId& msgId, ResultCallback callback){
    consumer.seekAsync(msgId, [callback](pulsar::Result result){
        py::gil_scoped_acquire acquire;
        callback(result);
    });
}

// TODO: implement async variant
void Consumer_seek_timestamp(Consumer& consumer, uint64_t timestamp) {
    waitForAsyncResult(
        [timestamp, &consumer](ResultCallback callback) { consumer.seekAsync(timestamp, callback); });
}

bool Consumer_is_connected(Consumer& consumer) { return consumer.isConnected(); }

MessageId Consumer_get_last_message_id(Consumer& consumer) {
    MessageId msgId;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = consumer.getLastMessageId(msgId);
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
    return msgId;
}

void export_consumer(py::module_& m) {
    py::class_<Consumer>(m, "Consumer")
        .def(py::init<>())
        .def("topic", &Consumer::getTopic, "return the topic this consumer is subscribed to",
             py::return_value_policy::copy)
        .def("subscription_name", &Consumer::getSubscriptionName, py::return_value_policy::copy)
        .def("consumer_name", &Consumer::getConsumerName, py::return_value_policy::copy)
        .def("unsubscribe", &Consumer_unsubscribe)
        .def("unsubscribe_async", &Consumer_unsubscribeAsync)
        .def("receive", &Consumer_receive)
        .def("receive", &Consumer_receive_timeout)
        .def("receive_async", &Consumer_receiveAsync)
        .def("batch_receive", &Consumer_batch_receive)
        .def("batch_receive_async", &Consumer_batch_receive_async)
        .def("acknowledge", &Consumer_acknowledge)
        .def("acknowledge", &Consumer_acknowledge_message_id)
        .def("acknowledge_async", &Consumer_acknowledgeAsync)
        .def("acknowledge_async", &Consumer_acknowledge_message_id_Async)
        .def("acknowledge_cumulative", &Consumer_acknowledge_cumulative)
        .def("acknowledge_cumulative", &Consumer_acknowledge_cumulative_message_id)
        .def("acknowledge_cumulative_async", &Consumer_acknowledge_cumulativeAsync)
        .def("negative_acknowledge", &Consumer_negative_acknowledge)
        .def("negative_acknowledge", &Consumer_negative_acknowledge_message_id)
        .def("close", &Consumer_close)
        .def("close_async", &Consumer_closeAsync)
        .def("pause_message_listener", &Consumer_pauseMessageListener)
        .def("resume_message_listener", &Consumer_resumeMessageListener)
        .def("redeliver_unacknowledged_messages", &Consumer::redeliverUnacknowledgedMessages)
        .def("seek", &Consumer_seek)
        .def("seek", &Consumer_seek_timestamp)
        .def("seek_async", Consumer_seekAsync)
        .def("is_connected", &Consumer_is_connected)
        .def("get_last_message_id", &Consumer_get_last_message_id);
}
