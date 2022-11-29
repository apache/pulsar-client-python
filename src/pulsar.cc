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

using Module = py::module_;

void export_client(Module& m);
void export_message(Module& m);
void export_producer(Module& m);
void export_consumer(Module& m);
void export_reader(Module& m);
void export_config(Module& m);
void export_enums(Module& m);
void export_authentication(Module& m);
void export_schema(Module& m);
void export_exceptions(Module& m);

PYBIND11_MODULE(_pulsar, m) {
    export_exceptions(m);
    export_client(m);
    export_message(m);
    export_producer(m);
    export_consumer(m);
    export_reader(m);
    export_config(m);
    export_enums(m);
    export_authentication(m);
    export_schema(m);
}
