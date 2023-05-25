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
#include <pybind11/pybind11.h>

namespace py = pybind11;

Message Reader_readNext(Reader& reader) {
    return waitForAsyncValue<Message>([&](ReadNextCallback callback) { reader.readNextAsync(callback); });
}

Message Reader_readNextTimeout(Reader& reader, int timeoutMs) {
    Message msg;
    py::gil_scoped_release release;
    CHECK_RESULT(reader.readNext(msg, timeoutMs));

    return msg;
}

bool Reader_hasMessageAvailable(Reader& reader) {
    return waitForAsyncValue<bool>(
        [&](HasMessageAvailableCallback callback) { reader.hasMessageAvailableAsync(callback); });
}

void Reader_close(Reader& reader) {
    waitForAsyncResult([&](ResultCallback callback) { reader.closeAsync(callback); });
}

void Reader_seek(Reader& reader, const MessageId& msgId) {
    waitForAsyncResult([&](ResultCallback callback) { reader.seekAsync(msgId, callback); });
}

void Reader_seek_timestamp(Reader& reader, uint64_t timestamp) {
    waitForAsyncResult([&](ResultCallback callback) { reader.seekAsync(timestamp, callback); });
}

bool Reader_is_connected(Reader& reader) { return reader.isConnected(); }

void export_reader(py::module_& m) {
    using namespace py;

    class_<Reader>(m, "Reader")
        .def("topic", &Reader::getTopic, return_value_policy::copy)
        .def("read_next", &Reader_readNext)
        .def("read_next", &Reader_readNextTimeout)
        .def("has_message_available", &Reader_hasMessageAvailable)
        .def("close", &Reader_close)
        .def("seek", &Reader_seek)
        .def("seek", &Reader_seek_timestamp)
        .def("is_connected", &Reader_is_connected);
}
