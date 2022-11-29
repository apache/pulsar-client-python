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
#include "exceptions.h"
#include <pybind11/pybind11.h>
#include <unordered_map>

using namespace pulsar;
namespace py = pybind11;

#define CASE_RESULT(className)      \
    case pulsar::Result##className: \
        throw className{pulsar::Result##className};

void raiseException(pulsar::Result result) {
    switch (result) {
        CASE_RESULT(UnknownError)
        CASE_RESULT(InvalidConfiguration)
        CASE_RESULT(Timeout)
        CASE_RESULT(LookupError)
        CASE_RESULT(ConnectError)
        CASE_RESULT(ReadError)
        CASE_RESULT(AuthenticationError)
        CASE_RESULT(AuthorizationError)
        CASE_RESULT(ErrorGettingAuthenticationData)
        CASE_RESULT(BrokerMetadataError)
        CASE_RESULT(BrokerPersistenceError)
        CASE_RESULT(ChecksumError)
        CASE_RESULT(ConsumerBusy)
        CASE_RESULT(NotConnected)
        CASE_RESULT(AlreadyClosed)
        CASE_RESULT(InvalidMessage)
        CASE_RESULT(ConsumerNotInitialized)
        CASE_RESULT(ProducerNotInitialized)
        CASE_RESULT(ProducerBusy)
        CASE_RESULT(TooManyLookupRequestException)
        CASE_RESULT(InvalidTopicName)
        CASE_RESULT(InvalidUrl)
        CASE_RESULT(ServiceUnitNotReady)
        CASE_RESULT(OperationNotSupported)
        CASE_RESULT(ProducerBlockedQuotaExceededError)
        CASE_RESULT(ProducerBlockedQuotaExceededException)
        CASE_RESULT(ProducerQueueIsFull)
        CASE_RESULT(MessageTooBig)
        CASE_RESULT(TopicNotFound)
        CASE_RESULT(SubscriptionNotFound)
        CASE_RESULT(ConsumerNotFound)
        CASE_RESULT(UnsupportedVersionError)
        CASE_RESULT(TopicTerminated)
        CASE_RESULT(CryptoError)
        CASE_RESULT(IncompatibleSchema)
        CASE_RESULT(ConsumerAssignError)
        CASE_RESULT(CumulativeAcknowledgementNotAllowedError)
        CASE_RESULT(TransactionCoordinatorNotFoundError)
        CASE_RESULT(InvalidTxnStatusError)
        CASE_RESULT(NotAllowedError)
        CASE_RESULT(TransactionConflict)
        CASE_RESULT(TransactionNotFound)
        CASE_RESULT(ProducerFenced)
        CASE_RESULT(MemoryBufferIsFull)
        CASE_RESULT(Interrupted)
        default:
            return;
    }
}

// There is no std::hash specification for an enum in Clang compiler of macOS for C++11
template <>
struct std::hash<Result> {
    std::size_t operator()(const Result& result) const noexcept {
        return std::hash<int>()(static_cast<int>(result));
    }
};

using PythonExceptionMap = std::unordered_map<Result, py::exception<PulsarException>>;
static PythonExceptionMap createPythonExceptionMap(py::module_& m, py::exception<PulsarException>& base) {
    PythonExceptionMap exceptions;
    exceptions[ResultUnknownError] = {m, "UnknownError", base};
    exceptions[ResultInvalidConfiguration] = {m, "InvalidConfiguration", base};
    exceptions[ResultTimeout] = {m, "Timeout", base};
    exceptions[ResultLookupError] = {m, "LookupError", base};
    exceptions[ResultConnectError] = {m, "ConnectError", base};
    exceptions[ResultReadError] = {m, "ReadError", base};
    exceptions[ResultAuthenticationError] = {m, "AuthenticationError", base};
    exceptions[ResultAuthorizationError] = {m, "AuthorizationError", base};
    exceptions[ResultErrorGettingAuthenticationData] = {m, "ErrorGettingAuthenticationData", base};
    exceptions[ResultBrokerMetadataError] = {m, "BrokerMetadataError", base};
    exceptions[ResultBrokerPersistenceError] = {m, "BrokerPersistenceError", base};
    exceptions[ResultChecksumError] = {m, "ChecksumError", base};
    exceptions[ResultConsumerBusy] = {m, "ConsumerBusy", base};
    exceptions[ResultNotConnected] = {m, "NotConnected", base};
    exceptions[ResultAlreadyClosed] = {m, "AlreadyClosed", base};
    exceptions[ResultInvalidMessage] = {m, "InvalidMessage", base};
    exceptions[ResultConsumerNotInitialized] = {m, "ConsumerNotInitialized", base};
    exceptions[ResultProducerNotInitialized] = {m, "ProducerNotInitialized", base};
    exceptions[ResultProducerBusy] = {m, "ProducerBusy", base};
    exceptions[ResultTooManyLookupRequestException] = {m, "TooManyLookupRequestException", base};
    exceptions[ResultInvalidTopicName] = {m, "InvalidTopicName", base};
    exceptions[ResultInvalidUrl] = {m, "InvalidUrl", base};
    exceptions[ResultServiceUnitNotReady] = {m, "ServiceUnitNotReady", base};
    exceptions[ResultOperationNotSupported] = {m, "OperationNotSupported", base};
    exceptions[ResultProducerBlockedQuotaExceededError] = {m, "ProducerBlockedQuotaExceededError", base};
    exceptions[ResultProducerBlockedQuotaExceededException] = {m, "ProducerBlockedQuotaExceededException",
                                                               base};
    exceptions[ResultProducerQueueIsFull] = {m, "ProducerQueueIsFull", base};
    exceptions[ResultMessageTooBig] = {m, "MessageTooBig", base};
    exceptions[ResultTopicNotFound] = {m, "TopicNotFound", base};
    exceptions[ResultSubscriptionNotFound] = {m, "SubscriptionNotFound", base};
    exceptions[ResultConsumerNotFound] = {m, "ConsumerNotFound", base};
    exceptions[ResultUnsupportedVersionError] = {m, "UnsupportedVersionError", base};
    exceptions[ResultTopicTerminated] = {m, "TopicTerminated", base};
    exceptions[ResultCryptoError] = {m, "CryptoError", base};
    exceptions[ResultIncompatibleSchema] = {m, "IncompatibleSchema", base};
    exceptions[ResultConsumerAssignError] = {m, "ConsumerAssignError", base};
    exceptions[ResultCumulativeAcknowledgementNotAllowedError] = {
        m, "CumulativeAcknowledgementNotAllowedError", base};
    exceptions[ResultTransactionCoordinatorNotFoundError] = {m, "TransactionCoordinatorNotFoundError", base};
    exceptions[ResultInvalidTxnStatusError] = {m, "InvalidTxnStatusError", base};
    exceptions[ResultNotAllowedError] = {m, "NotAllowedError", base};
    exceptions[ResultTransactionConflict] = {m, "TransactionConflict", base};
    exceptions[ResultTransactionNotFound] = {m, "TransactionNotFound", base};
    exceptions[ResultProducerFenced] = {m, "ProducerFenced", base};
    exceptions[ResultMemoryBufferIsFull] = {m, "MemoryBufferIsFull", base};
    exceptions[ResultInterrupted] = {m, "Interrupted", base};
    return exceptions;
}

void export_exceptions(py::module_& m) {
    static py::exception<PulsarException> base{m, "PulsarException"};
    static auto exceptions = createPythonExceptionMap(m, base);
    py::register_exception_translator([](std::exception_ptr e) {
        try {
            if (e) {
                std::rethrow_exception(e);
            }
        } catch (const PulsarException& e) {
            auto it = exceptions.find(e._result);
            if (it != exceptions.end()) {
                PyErr_SetString(it->second.ptr(), e.what());
            } else {
                base(e.what());
            }
        } catch (const std::invalid_argument& e) {
            PyErr_SetString(PyExc_ValueError, e.what());
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
    });
}
