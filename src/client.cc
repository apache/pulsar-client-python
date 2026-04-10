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

#include <pulsar/AutoClusterFailover.h>
#include <pulsar/ServiceInfoProvider.h>
#include <chrono>
#include <memory>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

static ServiceInfo unwrapPythonServiceInfo(const py::handle& object) {
    auto serviceInfoObject = py::reinterpret_borrow<py::object>(object);

    try {
        return serviceInfoObject.cast<ServiceInfo>();
    } catch (const py::cast_error&) {
    }

    if (py::hasattr(serviceInfoObject, "_service_info")) {
        try {
            return serviceInfoObject.attr("_service_info").cast<ServiceInfo>();
        } catch (const py::cast_error&) {
        }
    }

    throw py::value_error("Expected a pulsar.ServiceInfo or _pulsar.ServiceInfo instance");
}

class PythonServiceInfoProvider : public ServiceInfoProvider {
   public:
    explicit PythonServiceInfoProvider(py::object provider) : provider_(std::move(provider)) {}

    ~PythonServiceInfoProvider() override {
        if (!Py_IsInitialized()) {
            return;
        }

        py::gil_scoped_acquire acquire;
        try {
            if (py::hasattr(provider_, "close")) {
                provider_.attr("close")();
            }
        } catch (const py::error_already_set&) {
            PyErr_Print();
        }
    }

    ServiceInfo initialServiceInfo() override {
        py::gil_scoped_acquire acquire;
        return unwrapPythonServiceInfo(provider_.attr("initial_service_info")());
    }

    void initialize(std::function<void(ServiceInfo)> onServiceInfoUpdate) override {
        py::gil_scoped_acquire acquire;
        provider_.attr("initialize")(py::cpp_function(
            [onServiceInfoUpdate = std::move(onServiceInfoUpdate)](py::object serviceInfo) mutable {
                onServiceInfoUpdate(unwrapPythonServiceInfo(serviceInfo));
            }));
    }

   private:
    py::object provider_;
};

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

void Client_getTopicPartitionsAsync(Client& client, const std::string& topic,
                                    GetPartitionsCallback callback) {
    py::gil_scoped_release release;
    client.getPartitionsForTopicAsync(topic, callback);
}

SchemaInfo Client_getSchemaInfo(Client& client, const std::string& topic, int64_t version) {
    return waitForAsyncValue<SchemaInfo>([&](std::function<void(Result, const SchemaInfo&)> callback) {
        client.getSchemaInfoAsync(topic, version, callback);
    });
}

std::shared_ptr<Client> Client_createAutoClusterFailover(ServiceInfo primary,
                                                         std::vector<ServiceInfo> secondary,
                                                         int64_t checkIntervalMs, uint32_t failoverThreshold,
                                                         uint32_t switchBackThreshold,
                                                         const ClientConfiguration& conf) {
    AutoClusterFailover::Config autoClusterFailoverConfig(std::move(primary), std::move(secondary));
    autoClusterFailoverConfig.checkInterval = std::chrono::milliseconds(checkIntervalMs);
    autoClusterFailoverConfig.failoverThreshold = failoverThreshold;
    autoClusterFailoverConfig.switchBackThreshold = switchBackThreshold;
    return std::make_shared<Client>(
        Client::create(std::make_unique<AutoClusterFailover>(std::move(autoClusterFailoverConfig)), conf));
}

std::shared_ptr<Client> Client_createServiceInfoProvider(py::object provider,
                                                         const ClientConfiguration& conf) {
    return std::make_shared<Client>(
        Client::create(std::make_unique<PythonServiceInfoProvider>(std::move(provider)), conf));
}

void Client_close(Client& client) {
    waitForAsyncResult([&](ResultCallback callback) { client.closeAsync(callback); });
}

void Client_closeAsync(Client& client, ResultCallback callback) {
    py::gil_scoped_release release;
    client.closeAsync(callback);
}

void Client_subscribeAsync(Client& client, const std::string& topic, const std::string& subscriptionName,
                           ConsumerConfiguration conf, SubscribeCallback callback) {
    py::gil_scoped_release release;
    client.subscribeAsync(topic, subscriptionName, conf, callback);
}

void Client_subscribeAsync_topics(Client& client, const std::vector<std::string>& topics,
                                  const std::string& subscriptionName, ConsumerConfiguration conf,
                                  SubscribeCallback callback) {
    py::gil_scoped_release release;
    client.subscribeAsync(topics, subscriptionName, conf, callback);
}

void Client_subscribeAsync_pattern(Client& client, const std::string& topic_pattern,
                                   const std::string& subscriptionName, ConsumerConfiguration conf,
                                   SubscribeCallback callback) {
    py::gil_scoped_release release;
    client.subscribeWithRegexAsync(topic_pattern, subscriptionName, conf, callback);
}

void export_client(py::module_& m) {
    py::class_<Client, std::shared_ptr<Client>>(m, "Client")
        .def(py::init<const std::string&, const ClientConfiguration&>())
        .def_static("create_auto_cluster_failover", &Client_createAutoClusterFailover, py::arg("primary"),
                    py::arg("secondary"), py::arg("check_interval_ms"), py::arg("failover_threshold"),
                    py::arg("switch_back_threshold"), py::arg("client_configuration"))
        .def_static("create_service_info_provider", &Client_createServiceInfoProvider, py::arg("provider"),
                    py::arg("client_configuration"))
        .def("create_producer", &Client_createProducer)
        .def("create_producer_async", &Client_createProducerAsync)
        .def("subscribe", &Client_subscribe)
        .def("subscribe_topics", &Client_subscribe_topics)
        .def("subscribe_pattern", &Client_subscribe_pattern)
        .def("create_reader", &Client_createReader)
        .def("create_table_view",
             [](Client& client, const std::string& topic, const TableViewConfiguration& config) {
                 return waitForAsyncValue<TableView>([&](TableViewCallback callback) {
                     client.createTableViewAsync(topic, config, callback);
                 });
             })
        .def("get_topic_partitions", &Client_getTopicPartitions)
        .def("get_service_info", &Client::getServiceInfo)
        .def("get_schema_info", &Client_getSchemaInfo)
        .def("close", &Client_close)
        .def("close_async", &Client_closeAsync)
        .def("get_topic_partitions_async", &Client_getTopicPartitionsAsync)
        .def("subscribe_async", &Client_subscribeAsync)
        .def("subscribe_async_topics", &Client_subscribeAsync_topics)
        .def("subscribe_async_pattern", &Client_subscribeAsync_pattern)
        .def("shutdown", &Client::shutdown);
}
