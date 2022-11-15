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
#include <exception>
#include <Python.h>
#include "exceptions.h"
#include "future.h"

using namespace pulsar;

inline void CHECK_RESULT(Result res) {
    if (res != ResultOk) {
        raiseException(res);
    }
}

struct WaitForCallback {
    Promise<bool, Result> m_promise;

    WaitForCallback(Promise<bool, Result> promise) : m_promise(promise) {}

    void operator()(Result result) { m_promise.setValue(result); }
};

template <typename T>
struct WaitForCallbackValue {
    Promise<Result, T>& m_promise;

    WaitForCallbackValue(Promise<Result, T>& promise) : m_promise(promise) {}

    void operator()(Result result, const T& value) {
        if (result == ResultOk) {
            m_promise.setValue(value);
        } else {
            m_promise.setFailed(result);
        }
    }
};

void waitForAsyncResult(std::function<void(ResultCallback)> func);

template <typename T, typename Callback>
inline void waitForAsyncValue(std::function<void(Callback)> func, T& value) {
    Result res = ResultOk;
    Promise<Result, T> promise;
    Future<Result, T> future = promise.getFuture();

    Py_BEGIN_ALLOW_THREADS func(WaitForCallbackValue<T>(promise));
    Py_END_ALLOW_THREADS

        bool isComplete;
    while (true) {
        // Check periodically for Python signals
        Py_BEGIN_ALLOW_THREADS isComplete = future.get(res, std::ref(value), std::chrono::milliseconds(100));
        Py_END_ALLOW_THREADS

            if (isComplete) {
            CHECK_RESULT(res);
            return;
        }

        if (PyErr_CheckSignals() == -1) {
            PyErr_SetInterrupt();
            return;
        }
    }
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
