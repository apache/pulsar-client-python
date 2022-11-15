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
#include <pulsar/CompressionType.h>
#include <pulsar/ConsumerConfiguration.h>
#include <pulsar/ProducerConfiguration.h>
#include <pybind11/pybind11.h>

using namespace pulsar;
namespace py = pybind11;

void export_enums(py::module_& m) {
    using namespace py;

    enum_<ProducerConfiguration::PartitionsRoutingMode>(m, "PartitionsRoutingMode")
        .value("UseSinglePartition", ProducerConfiguration::UseSinglePartition)
        .value("RoundRobinDistribution", ProducerConfiguration::RoundRobinDistribution)
        .value("CustomPartition", ProducerConfiguration::CustomPartition);

    enum_<CompressionType>(m, "CompressionType")
        .value("NONE", CompressionNone)  // Don't use 'None' since it's a keyword in py3
        .value("LZ4", CompressionLZ4)
        .value("ZLib", CompressionZLib)
        .value("ZSTD", CompressionZSTD)
        .value("SNAPPY", CompressionSNAPPY);

    enum_<ConsumerType>(m, "ConsumerType")
        .value("Exclusive", ConsumerExclusive)
        .value("Shared", ConsumerShared)
        .value("Failover", ConsumerFailover)
        .value("KeyShared", ConsumerKeyShared);

    enum_<Result>(m, "Result", "Collection of return codes")
        .value("Ok", ResultOk)
        .value("UnknownError", ResultUnknownError)
        .value("InvalidConfiguration", ResultInvalidConfiguration)
        .value("Timeout", ResultTimeout)
        .value("LookupError", ResultLookupError)
        .value("ConnectError", ResultConnectError)
        .value("ReadError", ResultReadError)
        .value("AuthenticationError", ResultAuthenticationError)
        .value("AuthorizationError", ResultAuthorizationError)
        .value("ErrorGettingAuthenticationData", ResultErrorGettingAuthenticationData)
        .value("BrokerMetadataError", ResultBrokerMetadataError)
        .value("BrokerPersistenceError", ResultBrokerPersistenceError)
        .value("ChecksumError", ResultChecksumError)
        .value("ConsumerBusy", ResultConsumerBusy)
        .value("NotConnected", ResultNotConnected)
        .value("AlreadyClosed", ResultAlreadyClosed)
        .value("InvalidMessage", ResultInvalidMessage)
        .value("ConsumerNotInitialized", ResultConsumerNotInitialized)
        .value("ProducerNotInitialized", ResultProducerNotInitialized)
        .value("ProducerBusy", ResultProducerBusy)
        .value("TooManyLookupRequestException", ResultTooManyLookupRequestException)
        .value("InvalidTopicName", ResultInvalidTopicName)
        .value("InvalidUrl", ResultInvalidUrl)
        .value("ServiceUnitNotReady", ResultServiceUnitNotReady)
        .value("OperationNotSupported", ResultOperationNotSupported)
        .value("ProducerBlockedQuotaExceededError", ResultProducerBlockedQuotaExceededError)
        .value("ProducerBlockedQuotaExceededException", ResultProducerBlockedQuotaExceededException)
        .value("ProducerQueueIsFull", ResultProducerQueueIsFull)
        .value("MessageTooBig", ResultMessageTooBig)
        .value("TopicNotFound", ResultTopicNotFound)
        .value("SubscriptionNotFound", ResultSubscriptionNotFound)
        .value("ConsumerNotFound", ResultConsumerNotFound)
        .value("UnsupportedVersionError", ResultUnsupportedVersionError)
        .value("TopicTerminated", ResultTopicTerminated)
        .value("CryptoError", ResultCryptoError)
        .value("IncompatibleSchema", ResultIncompatibleSchema)
        .value("ConsumerAssignError", ResultConsumerAssignError)
        .value("CumulativeAcknowledgementNotAllowedError", ResultCumulativeAcknowledgementNotAllowedError)
        .value("TransactionCoordinatorNotFoundError", ResultTransactionCoordinatorNotFoundError)
        .value("InvalidTxnStatusError", ResultInvalidTxnStatusError)
        .value("NotAllowedError", ResultNotAllowedError)
        .value("TransactionConflict", ResultTransactionConflict)
        .value("TransactionNotFound", ResultTransactionNotFound)
        .value("ProducerFenced", ResultProducerFenced)
        .value("MemoryBufferIsFull", ResultMemoryBufferIsFull)
        .value("Interrupted", pulsar::ResultInterrupted);

    enum_<SchemaType>(m, "SchemaType", "Supported schema types")
        .value("NONE", pulsar::NONE)
        .value("STRING", pulsar::STRING)
        .value("INT8", pulsar::INT8)
        .value("INT16", pulsar::INT16)
        .value("INT32", pulsar::INT32)
        .value("INT64", pulsar::INT64)
        .value("FLOAT", pulsar::FLOAT)
        .value("DOUBLE", pulsar::DOUBLE)
        .value("BYTES", pulsar::BYTES)
        .value("JSON", pulsar::JSON)
        .value("PROTOBUF", pulsar::PROTOBUF)
        .value("AVRO", pulsar::AVRO)
        .value("AUTO_CONSUME", pulsar::AUTO_CONSUME)
        .value("AUTO_PUBLISH", pulsar::AUTO_PUBLISH)
        .value("KEY_VALUE", pulsar::KEY_VALUE);

    enum_<InitialPosition>(m, "InitialPosition", "Supported initial position")
        .value("Latest", InitialPositionLatest)
        .value("Earliest", InitialPositionEarliest);

    enum_<ProducerConfiguration::BatchingType>(m, "BatchingType", "Supported batching types")
        .value("Default", ProducerConfiguration::DefaultBatching)
        .value("KeyBased", ProducerConfiguration::KeyBasedBatching);

    enum_<Logger::Level>(m, "LoggerLevel")
        .value("Debug", Logger::LEVEL_DEBUG)
        .value("Info", Logger::LEVEL_INFO)
        .value("Warn", Logger::LEVEL_WARN)
        .value("Error", Logger::LEVEL_ERROR);
}
