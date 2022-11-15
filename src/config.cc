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
#include <pulsar/ClientConfiguration.h>
#include <pulsar/ConsoleLoggerFactory.h>
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/ProducerConfiguration.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <memory>

namespace py = pybind11;

#ifdef __GNUC__
#define HIDDEN __attribute__((visibility("hidden")))
#else
#define HIDDEN
#endif

class HIDDEN LoggerWrapper : public Logger, public CaptivePythonObjectMixin {
    const std::unique_ptr<Logger> _fallbackLogger;
    py::object _pyLogger;

   public:
    LoggerWrapper(PyObject* pyLoggerPtr, Logger* fallbackLogger, py::object pyLogger)
        : CaptivePythonObjectMixin(pyLoggerPtr), _fallbackLogger(fallbackLogger), _pyLogger(pyLogger) {}

    LoggerWrapper(const LoggerWrapper&) = delete;
    LoggerWrapper(LoggerWrapper&&) noexcept = delete;
    LoggerWrapper& operator=(const LoggerWrapper&) = delete;
    LoggerWrapper& operator=(LoggerWrapper&&) = delete;

    bool isEnabled(Level level) {
        return true;  // Python loggers are always enabled; they decide internally whether or not to log.
    }

    void log(Level level, int line, const std::string& message) {
        if (!Py_IsInitialized()) {
            // Python logger is unavailable - fallback to console logger
            _fallbackLogger->log(level, line, message);
        } else {
            PyGILState_STATE state = PyGILState_Ensure();
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            try {
                switch (level) {
                    case Logger::LEVEL_DEBUG:
                        _pyLogger(py::str("DEBUG"), message);
                        break;
                    case Logger::LEVEL_INFO:
                        _pyLogger(py::str("INFO"), message);
                        break;
                    case Logger::LEVEL_WARN:
                        _pyLogger(py::str("WARNING"), message);
                        break;
                    case Logger::LEVEL_ERROR:
                        _pyLogger(py::str("ERROR"), message);
                        break;
                }
            } catch (const py::error_already_set& e) {
                PyErr_Print();
                _fallbackLogger->log(level, line, message);
            }
            PyErr_Restore(type, value, traceback);
            PyGILState_Release(state);
        }
    }
};

class HIDDEN LoggerWrapperFactory : public LoggerFactory, public CaptivePythonObjectMixin {
    py::object _pyLogger;
    std::unique_ptr<LoggerFactory> _fallbackLoggerFactory{new ConsoleLoggerFactory};

   public:
    LoggerWrapperFactory(py::object pyLogger)
        : CaptivePythonObjectMixin(pyLogger.ptr()), _pyLogger(pyLogger) {}

    Logger* getLogger(const std::string& fileName) {
        const auto fallbackLogger = _fallbackLoggerFactory->getLogger(fileName);
        if (_captive == py::object().ptr()) {
            return fallbackLogger;
        } else {
            return new LoggerWrapper(_captive, fallbackLogger, _pyLogger);
        }
    }
};

static ClientConfiguration& ClientConfiguration_setLogger(ClientConfiguration& conf, py::object logger) {
    conf.setLogger(new LoggerWrapperFactory(logger));
    return conf;
}

static ClientConfiguration& ClientConfiguration_setConsoleLogger(ClientConfiguration& conf,
                                                                 Logger::Level level) {
    conf.setLogger(new ConsoleLoggerFactory(level));
    return conf;
}

static ClientConfiguration& ClientConfiguration_setFileLogger(ClientConfiguration& conf, Logger::Level level,
                                                              const std::string& logFile) {
    conf.setLogger(new FileLoggerFactory(level, logFile));
    return conf;
}

void export_config(py::module_& m) {
    using namespace py;

    class_<CryptoKeyReader, std::shared_ptr<CryptoKeyReader>>(m, "AbstractCryptoKeyReader")
        .def("getPublicKey", &CryptoKeyReader::getPublicKey)
        .def("getPrivateKey", &CryptoKeyReader::getPrivateKey);

    class_<DefaultCryptoKeyReader, CryptoKeyReader, std::shared_ptr<DefaultCryptoKeyReader>>(
        m, "CryptoKeyReader")
        .def(init<const std::string&, const std::string&>());

    class_<ClientConfiguration, std::shared_ptr<ClientConfiguration>>(m, "ClientConfiguration")
        .def(init<>())
        .def("authentication", &ClientConfiguration::setAuth, return_value_policy::reference)
        .def("operation_timeout_seconds", &ClientConfiguration::getOperationTimeoutSeconds)
        .def("operation_timeout_seconds", &ClientConfiguration::setOperationTimeoutSeconds,
             return_value_policy::reference)
        .def("connection_timeout", &ClientConfiguration::getConnectionTimeout)
        .def("connection_timeout", &ClientConfiguration::setConnectionTimeout, return_value_policy::reference)
        .def("io_threads", &ClientConfiguration::getIOThreads)
        .def("io_threads", &ClientConfiguration::setIOThreads, return_value_policy::reference)
        .def("message_listener_threads", &ClientConfiguration::getMessageListenerThreads)
        .def("message_listener_threads", &ClientConfiguration::setMessageListenerThreads,
             return_value_policy::reference)
        .def("concurrent_lookup_requests", &ClientConfiguration::getConcurrentLookupRequest)
        .def("concurrent_lookup_requests", &ClientConfiguration::setConcurrentLookupRequest,
             return_value_policy::reference)
        .def("log_conf_file_path", &ClientConfiguration::getLogConfFilePath, return_value_policy::copy)
        .def("log_conf_file_path", &ClientConfiguration::setLogConfFilePath, return_value_policy::reference)
        .def("use_tls", &ClientConfiguration::isUseTls)
        .def("use_tls", &ClientConfiguration::setUseTls, return_value_policy::reference)
        .def("tls_trust_certs_file_path", &ClientConfiguration::getTlsTrustCertsFilePath,
             return_value_policy::copy)
        .def("tls_trust_certs_file_path", &ClientConfiguration::setTlsTrustCertsFilePath,
             return_value_policy::reference)
        .def("tls_allow_insecure_connection", &ClientConfiguration::isTlsAllowInsecureConnection)
        .def("tls_allow_insecure_connection", &ClientConfiguration::setTlsAllowInsecureConnection,
             return_value_policy::reference)
        .def("tls_validate_hostname", &ClientConfiguration::setValidateHostName,
             return_value_policy::reference)
        .def("listener_name", &ClientConfiguration::setListenerName, return_value_policy::reference)
        .def("set_logger", &ClientConfiguration_setLogger, return_value_policy::reference)
        .def("set_console_logger", &ClientConfiguration_setConsoleLogger, return_value_policy::reference)
        .def("set_file_logger", &ClientConfiguration_setFileLogger, return_value_policy::reference);

    class_<ProducerConfiguration, std::shared_ptr<ProducerConfiguration>>(m, "ProducerConfiguration")
        .def(init<>())
        .def("producer_name", &ProducerConfiguration::getProducerName, return_value_policy::copy)
        .def("producer_name", &ProducerConfiguration::setProducerName, return_value_policy::reference)
        .def("schema", &ProducerConfiguration::getSchema, return_value_policy::copy)
        .def("schema", &ProducerConfiguration::setSchema, return_value_policy::reference)
        .def("send_timeout_millis", &ProducerConfiguration::getSendTimeout)
        .def("send_timeout_millis", &ProducerConfiguration::setSendTimeout, return_value_policy::reference)
        .def("initial_sequence_id", &ProducerConfiguration::getInitialSequenceId)
        .def("initial_sequence_id", &ProducerConfiguration::setInitialSequenceId,
             return_value_policy::reference)
        .def("compression_type", &ProducerConfiguration::getCompressionType)
        .def("compression_type", &ProducerConfiguration::setCompressionType, return_value_policy::reference)
        .def("max_pending_messages", &ProducerConfiguration::getMaxPendingMessages)
        .def("max_pending_messages", &ProducerConfiguration::setMaxPendingMessages,
             return_value_policy::reference)
        .def("max_pending_messages_across_partitions",
             &ProducerConfiguration::getMaxPendingMessagesAcrossPartitions)
        .def("max_pending_messages_across_partitions",
             &ProducerConfiguration::setMaxPendingMessagesAcrossPartitions, return_value_policy::reference)
        .def("block_if_queue_full", &ProducerConfiguration::getBlockIfQueueFull)
        .def("block_if_queue_full", &ProducerConfiguration::setBlockIfQueueFull,
             return_value_policy::reference)
        .def("partitions_routing_mode", &ProducerConfiguration::getPartitionsRoutingMode)
        .def("partitions_routing_mode", &ProducerConfiguration::setPartitionsRoutingMode,
             return_value_policy::reference)
        .def("lazy_start_partitioned_producers", &ProducerConfiguration::getLazyStartPartitionedProducers)
        .def("lazy_start_partitioned_producers", &ProducerConfiguration::setLazyStartPartitionedProducers,
             return_value_policy::reference)
        .def("batching_enabled", &ProducerConfiguration::getBatchingEnabled, return_value_policy::copy)
        .def("batching_enabled", &ProducerConfiguration::setBatchingEnabled, return_value_policy::reference)
        .def("batching_max_messages", &ProducerConfiguration::getBatchingMaxMessages,
             return_value_policy::copy)
        .def("batching_max_messages", &ProducerConfiguration::setBatchingMaxMessages,
             return_value_policy::reference)
        .def("batching_max_allowed_size_in_bytes", &ProducerConfiguration::getBatchingMaxAllowedSizeInBytes,
             return_value_policy::copy)
        .def("batching_max_allowed_size_in_bytes", &ProducerConfiguration::setBatchingMaxAllowedSizeInBytes,
             return_value_policy::reference)
        .def("batching_max_publish_delay_ms", &ProducerConfiguration::getBatchingMaxPublishDelayMs,
             return_value_policy::copy)
        .def("batching_max_publish_delay_ms", &ProducerConfiguration::setBatchingMaxPublishDelayMs,
             return_value_policy::reference)
        .def("chunking_enabled", &ProducerConfiguration::isChunkingEnabled)
        .def("chunking_enabled", &ProducerConfiguration::setChunkingEnabled, return_value_policy::reference)
        .def("property", &ProducerConfiguration::setProperty, return_value_policy::reference)
        .def("batching_type", &ProducerConfiguration::setBatchingType, return_value_policy::reference)
        .def("batching_type", &ProducerConfiguration::getBatchingType)
        .def("encryption_key", &ProducerConfiguration::addEncryptionKey, return_value_policy::reference)
        .def("crypto_key_reader", &ProducerConfiguration::setCryptoKeyReader, return_value_policy::reference);

    class_<BatchReceivePolicy>(m, "BatchReceivePolicy")
        .def(init<int, int, long>())
        .def("getTimeoutMs", &BatchReceivePolicy::getTimeoutMs)
        .def("getMaxNumMessages", &BatchReceivePolicy::getMaxNumMessages)
        .def("getMaxNumBytes", &BatchReceivePolicy::getMaxNumBytes);

    class_<ConsumerConfiguration, std::shared_ptr<ConsumerConfiguration>>(m, "ConsumerConfiguration")
        .def(init<>())
        .def("consumer_type", &ConsumerConfiguration::getConsumerType)
        .def("consumer_type", &ConsumerConfiguration::setConsumerType, return_value_policy::reference)
        .def("schema", &ConsumerConfiguration::getSchema, return_value_policy::copy)
        .def("schema", &ConsumerConfiguration::setSchema, return_value_policy::reference)
        .def("message_listener", &ConsumerConfiguration::setMessageListener, return_value_policy::reference)
        .def("receiver_queue_size", &ConsumerConfiguration::getReceiverQueueSize)
        .def("receiver_queue_size", &ConsumerConfiguration::setReceiverQueueSize)
        .def("max_total_receiver_queue_size_across_partitions",
             &ConsumerConfiguration::getMaxTotalReceiverQueueSizeAcrossPartitions)
        .def("max_total_receiver_queue_size_across_partitions",
             &ConsumerConfiguration::setMaxTotalReceiverQueueSizeAcrossPartitions)
        .def("batch_receive_policy", &ConsumerConfiguration::getBatchReceivePolicy, return_value_policy::copy)
        .def("batch_receive_policy", &ConsumerConfiguration::setBatchReceivePolicy)
        .def("consumer_name", &ConsumerConfiguration::getConsumerName, return_value_policy::copy)
        .def("consumer_name", &ConsumerConfiguration::setConsumerName)
        .def("unacked_messages_timeout_ms", &ConsumerConfiguration::getUnAckedMessagesTimeoutMs)
        .def("unacked_messages_timeout_ms", &ConsumerConfiguration::setUnAckedMessagesTimeoutMs)
        .def("negative_ack_redelivery_delay_ms", &ConsumerConfiguration::getNegativeAckRedeliveryDelayMs)
        .def("negative_ack_redelivery_delay_ms", &ConsumerConfiguration::setNegativeAckRedeliveryDelayMs)
        .def("broker_consumer_stats_cache_time_ms",
             &ConsumerConfiguration::getBrokerConsumerStatsCacheTimeInMs)
        .def("broker_consumer_stats_cache_time_ms",
             &ConsumerConfiguration::setBrokerConsumerStatsCacheTimeInMs)
        .def("pattern_auto_discovery_period", &ConsumerConfiguration::getPatternAutoDiscoveryPeriod)
        .def("pattern_auto_discovery_period", &ConsumerConfiguration::setPatternAutoDiscoveryPeriod)
        .def("read_compacted", &ConsumerConfiguration::isReadCompacted)
        .def("read_compacted", &ConsumerConfiguration::setReadCompacted)
        .def("property", &ConsumerConfiguration::setProperty, return_value_policy::reference)
        .def("subscription_initial_position", &ConsumerConfiguration::getSubscriptionInitialPosition)
        .def("subscription_initial_position", &ConsumerConfiguration::setSubscriptionInitialPosition)
        .def("crypto_key_reader", &ConsumerConfiguration::setCryptoKeyReader, return_value_policy::reference)
        .def("replicate_subscription_state_enabled",
             &ConsumerConfiguration::setReplicateSubscriptionStateEnabled)
        .def("replicate_subscription_state_enabled",
             &ConsumerConfiguration::isReplicateSubscriptionStateEnabled)
        .def("max_pending_chunked_message", &ConsumerConfiguration::getMaxPendingChunkedMessage)
        .def("max_pending_chunked_message", &ConsumerConfiguration::setMaxPendingChunkedMessage,
             return_value_policy::reference)
        .def("auto_ack_oldest_chunked_message_on_queue_full",
             &ConsumerConfiguration::isAutoAckOldestChunkedMessageOnQueueFull)
        .def("auto_ack_oldest_chunked_message_on_queue_full",
             &ConsumerConfiguration::setAutoAckOldestChunkedMessageOnQueueFull,
             return_value_policy::reference)
        .def("start_message_id_inclusive", &ConsumerConfiguration::isStartMessageIdInclusive)
        .def("start_message_id_inclusive", &ConsumerConfiguration::setStartMessageIdInclusive,
             return_value_policy::reference);

    class_<ReaderConfiguration, std::shared_ptr<ReaderConfiguration>>(m, "ReaderConfiguration")
        .def(init<>())
        .def("reader_listener", &ReaderConfiguration::setReaderListener, return_value_policy::reference)
        .def("schema", &ReaderConfiguration::getSchema, return_value_policy::copy)
        .def("schema", &ReaderConfiguration::setSchema, return_value_policy::reference)
        .def("receiver_queue_size", &ReaderConfiguration::getReceiverQueueSize)
        .def("receiver_queue_size", &ReaderConfiguration::setReceiverQueueSize)
        .def("reader_name", &ReaderConfiguration::getReaderName, return_value_policy::copy)
        .def("reader_name", &ReaderConfiguration::setReaderName)
        .def("subscription_role_prefix", &ReaderConfiguration::getSubscriptionRolePrefix,
             return_value_policy::copy)
        .def("subscription_role_prefix", &ReaderConfiguration::setSubscriptionRolePrefix)
        .def("read_compacted", &ReaderConfiguration::isReadCompacted)
        .def("read_compacted", &ReaderConfiguration::setReadCompacted)
        .def("crypto_key_reader", &ReaderConfiguration::setCryptoKeyReader, return_value_policy::reference);
}
