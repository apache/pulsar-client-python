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
#pragma once

#include <pulsar/Result.h>

#include <exception>
#include <string>

struct PulsarException : std::exception {
    const pulsar::Result _result;
    std::string _msg = "Pulsar error: ";
    PulsarException(pulsar::Result res) : _result(res) { _msg += strResult(res); }
    const char* what() const noexcept override { return _msg.c_str(); }
};

void raiseException(pulsar::Result result);

#define INHERIT_PULSAR_EXCEPTION(name)          \
    struct name : PulsarException {             \
        using PulsarException::PulsarException; \
    };

INHERIT_PULSAR_EXCEPTION(UnknownError)
INHERIT_PULSAR_EXCEPTION(InvalidConfiguration)
INHERIT_PULSAR_EXCEPTION(Timeout)
INHERIT_PULSAR_EXCEPTION(LookupError)
INHERIT_PULSAR_EXCEPTION(ConnectError)
INHERIT_PULSAR_EXCEPTION(ReadError)
INHERIT_PULSAR_EXCEPTION(AuthenticationError)
INHERIT_PULSAR_EXCEPTION(AuthorizationError)
INHERIT_PULSAR_EXCEPTION(ErrorGettingAuthenticationData)
INHERIT_PULSAR_EXCEPTION(BrokerMetadataError)
INHERIT_PULSAR_EXCEPTION(BrokerPersistenceError)
INHERIT_PULSAR_EXCEPTION(ChecksumError)
INHERIT_PULSAR_EXCEPTION(ConsumerBusy)
INHERIT_PULSAR_EXCEPTION(NotConnected)
INHERIT_PULSAR_EXCEPTION(AlreadyClosed)
INHERIT_PULSAR_EXCEPTION(InvalidMessage)
INHERIT_PULSAR_EXCEPTION(ConsumerNotInitialized)
INHERIT_PULSAR_EXCEPTION(ProducerNotInitialized)
INHERIT_PULSAR_EXCEPTION(ProducerBusy)
INHERIT_PULSAR_EXCEPTION(TooManyLookupRequestException)
INHERIT_PULSAR_EXCEPTION(InvalidTopicName)
INHERIT_PULSAR_EXCEPTION(InvalidUrl)
INHERIT_PULSAR_EXCEPTION(ServiceUnitNotReady)
INHERIT_PULSAR_EXCEPTION(OperationNotSupported)
INHERIT_PULSAR_EXCEPTION(ProducerBlockedQuotaExceededError)
INHERIT_PULSAR_EXCEPTION(ProducerBlockedQuotaExceededException)
INHERIT_PULSAR_EXCEPTION(ProducerQueueIsFull)
INHERIT_PULSAR_EXCEPTION(MessageTooBig)
INHERIT_PULSAR_EXCEPTION(TopicNotFound)
INHERIT_PULSAR_EXCEPTION(SubscriptionNotFound)
INHERIT_PULSAR_EXCEPTION(ConsumerNotFound)
INHERIT_PULSAR_EXCEPTION(UnsupportedVersionError)
INHERIT_PULSAR_EXCEPTION(TopicTerminated)
INHERIT_PULSAR_EXCEPTION(CryptoError)
INHERIT_PULSAR_EXCEPTION(IncompatibleSchema)
INHERIT_PULSAR_EXCEPTION(ConsumerAssignError)
INHERIT_PULSAR_EXCEPTION(CumulativeAcknowledgementNotAllowedError)
INHERIT_PULSAR_EXCEPTION(TransactionCoordinatorNotFoundError)
INHERIT_PULSAR_EXCEPTION(InvalidTxnStatusError)
INHERIT_PULSAR_EXCEPTION(NotAllowedError)
INHERIT_PULSAR_EXCEPTION(TransactionConflict)
INHERIT_PULSAR_EXCEPTION(TransactionNotFound)
INHERIT_PULSAR_EXCEPTION(ProducerFenced)
INHERIT_PULSAR_EXCEPTION(MemoryBufferIsFull)
INHERIT_PULSAR_EXCEPTION(Interrupted)

#undef INHERIT_PULSAR_EXCEPTION
