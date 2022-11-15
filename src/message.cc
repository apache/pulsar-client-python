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

#include <pybind11/chrono.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sstream>

namespace py = pybind11;

void export_message(py::module_& m) {
    using namespace py;

    PyDateTime_IMPORT;

    MessageBuilder& (MessageBuilder::*MessageBuilderSetContentString)(const std::string&) =
        &MessageBuilder::setContent;

    class_<MessageBuilder>(m, "MessageBuilder")
        .def(init<>())
        .def("content", MessageBuilderSetContentString, return_value_policy::reference)
        .def("property", &MessageBuilder::setProperty, return_value_policy::reference)
        .def("properties", &MessageBuilder::setProperties, return_value_policy::reference)
        .def("sequence_id", &MessageBuilder::setSequenceId, return_value_policy::reference)
        .def("deliver_after", &MessageBuilder::setDeliverAfter, return_value_policy::reference)
        .def("deliver_at", &MessageBuilder::setDeliverAt, return_value_policy::reference)
        .def("partition_key", &MessageBuilder::setPartitionKey, return_value_policy::reference)
        .def("event_timestamp", &MessageBuilder::setEventTimestamp, return_value_policy::reference)
        .def("replication_clusters", &MessageBuilder::setReplicationClusters, return_value_policy::reference)
        .def("disable_replication", &MessageBuilder::disableReplication, return_value_policy::reference)
        .def("build", &MessageBuilder::build);

    class_<MessageId>(m, "MessageId")
        .def(init<int32_t, int64_t, int64_t, int32_t>())
        .def("__str__",
             [](const MessageId& msgId) {
                 std::ostringstream oss;
                 oss << msgId;
                 return oss.str();
             })
        .def("__eq__", &MessageId::operator==)
        .def("__ne__", &MessageId::operator!=)
        .def("__le__", &MessageId::operator<=)
        .def("__lt__", &MessageId::operator<)
        .def("__ge__", &MessageId::operator>=)
        .def("__gt__", &MessageId::operator>)
        .def("ledger_id", &MessageId::ledgerId)
        .def("entry_id", &MessageId::entryId)
        .def("batch_index", &MessageId::batchIndex)
        .def("partition", &MessageId::partition)
        .def_property_readonly_static("earliest", [](object) { return MessageId::earliest(); })
        .def_property_readonly_static("latest", [](object) { return MessageId::latest(); })
        .def("serialize",
             [](const MessageId& msgId) {
                 std::string serialized;
                 msgId.serialize(serialized);
                 return bytes(serialized);
             })
        .def_static("deserialize", &MessageId::deserialize);

    class_<Message>(m, "Message")
        .def(init<>())
        .def("properties", &Message::getProperties)
        .def("data", [](const Message& msg) { return bytes(msg.getDataAsString()); })
        .def("length", &Message::getLength)
        .def("partition_key", &Message::getPartitionKey, return_value_policy::copy)
        .def("publish_timestamp", &Message::getPublishTimestamp)
        .def("event_timestamp", &Message::getEventTimestamp)
        .def("message_id", &Message::getMessageId, return_value_policy::copy)
        .def("__str__",
             [](const Message& msg) {
                 std::ostringstream oss;
                 oss << msg;
                 return oss.str();
             })
        .def("topic_name", &Message::getTopicName, return_value_policy::copy)
        .def("redelivery_count", &Message::getRedeliveryCount)
        .def("schema_version", &Message::getSchemaVersion, return_value_policy::copy);

    MessageBatch& (MessageBatch::*MessageBatchParseFromString)(const std::string& payload,
                                                               uint32_t batchSize) = &MessageBatch::parseFrom;

    class_<MessageBatch>(m, "MessageBatch")
        .def(init<>())
        .def("with_message_id", &MessageBatch::withMessageId, return_value_policy::reference)
        .def("parse_from", MessageBatchParseFromString, return_value_policy::reference)
        .def("messages", &MessageBatch::messages, return_value_policy::copy);
}
