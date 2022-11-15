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
#include <pulsar/Authentication.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
using namespace pulsar;

void export_authentication(py::module_& m) {
    using namespace py;

    class_<Authentication, std::shared_ptr<Authentication>>(m, "Authentication")
        .def("getAuthMethodName", &Authentication::getAuthMethodName)
        .def("getAuthData", &Authentication::getAuthData)
        .def_static("create", static_cast<AuthenticationPtr (*)(const std::string&, const std::string&)>(
                                  &AuthFactory::create));

    class_<AuthTls, Authentication, std::shared_ptr<AuthTls>>(m, "AuthenticationTLS")
        .def(init<AuthenticationDataPtr&>())
        .def_static("create", static_cast<AuthenticationPtr (*)(const std::string&, const std::string&)>(
                                  &AuthTls::create));

    class_<AuthToken, Authentication, std::shared_ptr<AuthToken>>(m, "AuthenticationToken")
        .def(init<AuthenticationDataPtr&>())
        .def_static("create", static_cast<AuthenticationPtr (*)(const TokenSupplier&)>(&AuthToken::create))
        .def_static("create", static_cast<AuthenticationPtr (*)(const std::string&)>(&AuthToken::create));

    class_<AuthAthenz, Authentication, std::shared_ptr<AuthAthenz>>(m, "AuthenticationAthenz")
        .def(init<AuthenticationDataPtr&>())
        .def_static("create", static_cast<AuthenticationPtr (*)(const std::string&)>(&AuthAthenz::create));

    class_<AuthOauth2, Authentication, std::shared_ptr<AuthOauth2>>(m, "AuthenticationOauth2")
        .def(init<ParamMap&>())
        .def_static("create", static_cast<AuthenticationPtr (*)(const std::string&)>(&AuthOauth2::create));

    class_<AuthBasic, Authentication, std::shared_ptr<AuthBasic>>(m, "AuthenticationBasic")
        .def(init<AuthenticationDataPtr&>())
        .def_static("create", static_cast<AuthenticationPtr (*)(
                                  const std::string& /* username */, const std::string& /* password */,
                                  const std::string& /* method */)>(&AuthBasic::create))
        .def_static("create", static_cast<AuthenticationPtr (*)(const std::string&)>(&AuthBasic::create));
}
