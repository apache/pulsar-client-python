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

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

Producer Client_createProducer(Client& client, const std::string& topic, const ProducerConfiguration& conf) {
    return waitForAsyncValue<Producer>(
        [&](CreateProducerCallback callback) { client.createProducerAsync(topic, conf, callback); });
}

void Client_createProducerAsync(Client& client, const std::string& topic, ProducerConfiguration conf,
                                CreateProducerCallback callback) {
    py::gil_scoped_release release;
    client.createProducerAsync(topic, conf, callback);
}

Consumer Client_subscribe(Client& client, const std::string& topic, const std::string& subscriptionName,
                          const ConsumerConfiguration& conf) {
    return waitForAsyncValue<Consumer>(
        [&](SubscribeCallback callback) { client.subscribeAsync(topic, subscriptionName, conf, callback); });
}

void Client_subscribeAsync(Client& client, const std::string& topic, const std::string& subscriptionName,
                          const ConsumerConfiguration& conf, SubscribeCallback callback) {
    py::gil_scoped_release release;
    client.subscribeAsync(topic, subscriptionName, conf, callback);
}

Consumer Client_subscribe_topics(Client& client, const std::vector<std::string>& topics,
                                 const std::string& subscriptionName, const ConsumerConfiguration& conf) {
    return waitForAsyncValue<Consumer>(
        [&](SubscribeCallback callback) { client.subscribeAsync(topics, subscriptionName, conf, callback); });
}

Consumer Client_subscribe_pattern(Client& client, const std::string& topic_pattern,
                                  const std::string& subscriptionName, const ConsumerConfiguration& conf) {
    return waitForAsyncValue<Consumer>([&](SubscribeCallback callback) {
        client.subscribeWithRegexAsync(topic_pattern, subscriptionName, conf, callback);
    });
}

Reader Client_createReader(Client& client, const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf) {
    return waitForAsyncValue<Reader>(
        [&](ReaderCallback callback) { client.createReaderAsync(topic, startMessageId, conf, callback); });
}

std::vector<std::string> Client_getTopicPartitions(Client& client, const std::string& topic) {
    return waitForAsyncValue<std::vector<std::string>>(
        [&](GetPartitionsCallback callback) { client.getPartitionsForTopicAsync(topic, callback); });
}

SchemaInfo Client_getSchemaInfo(Client& client, const std::string& topic, int64_t version) {
    return waitForAsyncValue<SchemaInfo>([&](std::function<void(Result, const SchemaInfo&)> callback) {
        client.getSchemaInfoAsync(topic, version, callback);
    });
}

void Client_close(Client& client) {
    waitForAsyncResult([&](ResultCallback callback) { client.closeAsync(callback); });
}

void Client_closeAsync(Client& client, ResultCallback callback) {
    py::gil_scoped_release release;
    client.closeAsync(callback);
}

void export_client(py::module_& m) {
    py::class_<Client, std::shared_ptr<Client>>(m, "Client")
        .def(py::init<const std::string&, const ClientConfiguration&>())
        .def("create_producer", &Client_createProducer)
        .def("create_producer_async", &Client_createProducerAsync)
        .def("subscribe", &Client_subscribe)
        .def("subscribe_async", &Client_subscribeAsync)
        .def("subscribe_topics", &Client_subscribe_topics)
        .def("subscribe_pattern", &Client_subscribe_pattern)
        .def("create_reader", &Client_createReader)
        .def("get_topic_partitions", &Client_getTopicPartitions)
        .def("get_schema_info", &Client_getSchemaInfo)
        .def("close", &Client_close)
        .def("close_async", &Client_closeAsync)
        .def("shutdown", &Client::shutdown);
}
