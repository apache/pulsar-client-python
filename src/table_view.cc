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
#include <pybind11/pybind11.h>
#include <pulsar/TableView.h>
#include <pulsar/Schema.h>
#include <pulsar/TableViewConfiguration.h>
#include <pybind11/stl.h>
#include <utility>
#include "utils.h"

namespace py = pybind11;
using namespace pulsar;

void export_table_view(py::module_& m) {
    py::class_<TableViewConfiguration>(m, "TableViewConfiguration")
        .def(py::init<>())
        .def("subscription_name",
             [](TableViewConfiguration& config, const std::string& name) { config.subscriptionName = name; })
        .def("schema",
             [](TableViewConfiguration& config, const SchemaInfo& schema) { config.schemaInfo = schema; });

    py::class_<TableView>(m, "TableView")
        .def(py::init<>())
        .def("get",
             [](const TableView& view, const std::string& key) -> std::pair<bool, py::bytes> {
                 py::gil_scoped_release release;
                 std::string value;
                 bool available = view.getValue(key, value);
                 py::gil_scoped_acquire acquire;
                 if (available) {
                     return std::make_pair(true, py::bytes(std::move(value)));
                 } else {
                     return std::make_pair(false, py::bytes());
                 }
             })
        .def("size", &TableView::size, py::call_guard<py::gil_scoped_release>())
        .def("close", [](TableView& view) {
            waitForAsyncResult([&view](ResultCallback callback) { view.closeAsync(callback); });
        });
}
