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

#include <pulsar/Client.h>
#include <pulsar/MessageBatch.h>
#include <chrono>
#include <exception>
#include <future>
#include <pybind11/pybind11.h>
#include "exceptions.h"

using namespace pulsar;
namespace py = pybind11;

inline void CHECK_RESULT(Result res) {
    if (res != ResultOk) {
        raiseException(res);
    }
}

namespace internal {

void waitForResult(std::promise<pulsar::Result>& promise);

}  // namespace internal

void waitForAsyncResult(std::function<void(ResultCallback)> func);

template <typename T>
inline T waitForAsyncValue(std::function<void(std::function<void(Result, const T&)>)> func) {
    auto resultPromise = std::make_shared<std::promise<Result>>();
    auto valuePromise = std::make_shared<std::promise<T>>();

    {
        py::gil_scoped_release release;

        func([resultPromise, valuePromise](Result result, const T& value) {
            valuePromise->set_value(value);
            resultPromise->set_value(result);
        });
    }

    internal::waitForResult(*resultPromise);
    return valuePromise->get_future().get();
}

struct CryptoKeyReaderWrapper {
    CryptoKeyReaderPtr cryptoKeyReader;

    CryptoKeyReaderWrapper();
    CryptoKeyReaderWrapper(const std::string& publicKeyPath, const std::string& privateKeyPath);
};

class CaptivePythonObjectMixin {
   protected:
    PyObject* _captive;

    CaptivePythonObjectMixin(PyObject* captive) {
        _captive = captive;
        PyGILState_STATE state = PyGILState_Ensure();
        Py_XINCREF(_captive);
        PyGILState_Release(state);
    }

    ~CaptivePythonObjectMixin() {
        if (Py_IsInitialized()) {
            PyGILState_STATE state = PyGILState_Ensure();
            Py_XDECREF(_captive);
            PyGILState_Release(state);
        }
    }
};
