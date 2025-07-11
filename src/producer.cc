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

#include <functional>
#include <pybind11/pybind11.h>
#include <pybind11/functional.h>

namespace py = pybind11;

MessageId Producer_send(Producer& producer, const Message& message) {
    return waitForAsyncValue<MessageId>(
        [&](SendCallback callback) { producer.sendAsync(message, callback); });
}

void Producer_sendAsync(Producer& producer, const Message& msg, SendCallback callback) {
    Py_BEGIN_ALLOW_THREADS producer.sendAsync(msg, callback);
    Py_END_ALLOW_THREADS

        if (PyErr_CheckSignals() == -1) {
        PyErr_SetInterrupt();
    }
}

// TODO: implement async variant
void Producer_flush(Producer& producer) {
    waitForAsyncResult([&](ResultCallback callback) { producer.flushAsync(callback); });
}

void Producer_close(Producer& producer) {
    waitForAsyncResult([&](ResultCallback callback) { producer.closeAsync(callback); });
}

void Producer_closeAsync(Producer& producer, ResultCallback callback) {
    py::gil_scoped_release release;
    producer.closeAsync(callback);
}

void export_producer(py::module_& m) {
    using namespace py;

    class_<Producer>(m, "Producer")
        .def(init<>())
        .def("topic", &Producer::getTopic, "return the topic to which producer is publishing to",
             return_value_policy::copy)
        .def("producer_name", &Producer::getProducerName,
             "return the producer name which could have been assigned by the system or specified by the "
             "client",
             return_value_policy::copy)
        .def("last_sequence_id", &Producer::getLastSequenceId)
        .def("send", &Producer_send,
             "Publish a message on the topic associated with this Producer.\n"
             "\n"
             "This method will block until the message will be accepted and persisted\n"
             "by the broker. In case of errors, the client library will try to\n"
             "automatically recover and use a different broker.\n"
             "\n"
             "If it wasn't possible to successfully publish the message within the sendTimeout,\n"
             "an error will be returned.\n"
             "\n"
             "This method is equivalent to asyncSend() and wait until the callback is triggered.\n"
             "\n"
             "@param msg message to publish\n")
        .def("send_async", &Producer_sendAsync)
        .def("flush", &Producer_flush,
             "Flush all the messages buffered in the client and wait until all messages have been\n"
             "successfully persisted\n")
        .def("close", &Producer_close)
        .def("close_async", &Producer_closeAsync)
        .def("is_connected", &Producer::isConnected);
}
