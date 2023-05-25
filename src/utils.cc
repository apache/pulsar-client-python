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

void waitForAsyncResult(std::function<void(ResultCallback)> func) {
    auto promise = std::make_shared<std::promise<Result>>();

    {
        // Always call the Pulsar C++ client methods without holding
        // the GIL. This avoids deadlocks due the sequence of acquiring
        // mutexes by different threads. eg:
        // Thread-1: GIL -> producer.lock
        // Thread-2: producer.lock -> GIL (In a callback)
        py::gil_scoped_release release;
        func([promise](Result result) { promise->set_value(result); });
    }
    internal::waitForResult(*promise);
}

namespace internal {

void waitForResult(std::promise<pulsar::Result>& promise) {
    auto future = promise.get_future();
    while (true) {
        {
            py::gil_scoped_release release;
            auto status = future.wait_for(std::chrono::milliseconds(100));
            if (status == std::future_status::ready) {
                CHECK_RESULT(future.get());
                return;
            }
        }
        py::gil_scoped_acquire acquire;
        if (PyErr_CheckSignals() != 0) {
            raiseException(ResultInterrupted);
        }
    }
}

}  // namespace internal
