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

namespace py = pybind11;

Message Reader_readNext(Reader& reader) {
    return waitForAsyncValue<Message>([&](ReadNextCallback callback) { reader.readNextAsync(callback); });
}

Message Reader_readNextTimeout(Reader& reader, int timeoutMs) {
    Message msg;
    Result res;
    Py_BEGIN_ALLOW_THREADS res = reader.readNext(msg, timeoutMs);
    Py_END_ALLOW_THREADS

        CHECK_RESULT(res);
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

void Reader_readNextAsync(Reader& reader, ReadNextCallback callback) {
    py::gil_scoped_release release;
    reader.readNextAsync(callback);
}

void Reader_closeAsync(Reader& reader, ResultCallback callback) {
    py::gil_scoped_release release;
    reader.closeAsync(callback);
}

void Reader_seekAsync(Reader& reader, const MessageId& msgId, ResultCallback callback) {
    py::gil_scoped_release release;
    reader.seekAsync(msgId, callback);
}

void Reader_seekAsync_timestamp(Reader& reader, uint64_t timestamp, ResultCallback callback) {
    py::gil_scoped_release release;
    reader.seekAsync(timestamp, callback);
}

void Reader_hasMessageAvailableAsync(Reader& reader, HasMessageAvailableCallback callback) {
    py::gil_scoped_release release;
    reader.hasMessageAvailableAsync(callback);
}

void export_reader(py::module_& m) {
    using namespace py;

    class_<Reader>(m, "Reader")
        .def("topic", &Reader::getTopic, return_value_policy::copy)
        .def("read_next", &Reader_readNext)
        .def("read_next", &Reader_readNextTimeout)
        .def("read_next_async", &Reader_readNextAsync)
        .def("has_message_available", &Reader_hasMessageAvailable)
        .def("has_message_available_async", &Reader_hasMessageAvailableAsync)
        .def("close", &Reader_close)
        .def("close_async", &Reader_closeAsync)
        .def("seek", &Reader_seek)
        .def("seek", &Reader_seek_timestamp)
        .def("seek_async", &Reader_seekAsync)
        .def("seek_async", &Reader_seekAsync_timestamp)
        .def("is_connected", &Reader_is_connected);
}
