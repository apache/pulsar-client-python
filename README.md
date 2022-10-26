<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar Python client library

## Requirements

- Python >= 3.7
- A C++ compiler that supports C++11
- CMake >= 3.18
- [Pulsar C++ client library](https://github.com/apache/pulsar-client-cpp)
- [Boost.Python](https://github.com/boostorg/python)

## Install the Python wheel

### Windows (with Vcpkg)

First, install the dependencies via [Vcpkg](https://github.com/microsoft/vcpkg).

```PowerShell
vcpkg install --feature-flags=manifests --triplet x64-windows
```

> NOTE: For Windows 32-bit library, change `x64-windows` to `x86-windows`, see [here](https://github.com/microsoft/vcpkg/tree/master/triplets) for all available triplets.

Then, build and install the Python wheel.

```PowerShell
# Assuming the Pulsar C++ client has been installed under the `PULSAR_CPP` directory.
cmake -B build -DUSE_VCPKG=ON -DCMAKE_PREFIX_PATH="$env:PULSAR_CPP" -DLINK_STATIC=ON
cmake --build build --config Release
cmake --install build
py setup.py bdist_wheel
py -m pip install ./dist/pulsar_client-*.whl
```

Since the Python client links to Boost.Python dynamically, you have to copy the dll (e.g. `boost_python310-vc142-mt-x64-1_80.dll`) into the system path (the `PATH` environment variable). If the `-DLINK_STATIC=ON` option is not specified, you have to copy the `pulsar.dll` into the system path as well.

### Linux or macOS

Assuming the Pulsar C++ client and Boost.Python have been installed under the system path.

```bash
cmake -B build
cmake --build build -j8
cmake --install build
./setup.py bdist_wheel
pip3 install dist/pulsar_client-*.whl --force-reinstall
```

> **NOTE**
>
> 1. Here a separate `build` directory is created to store all CMake temporary files. However, the `setup.py` requires the `_pulsar.so` is under the project directory.
> 2. Add the `--force-reinstall` option to overwrite the existing Python wheel in case your system has already installed a wheel before.

## Running examples

You can run `python3 -c 'import pulsar'` to see whether the wheel has been installed successfully. If it failed, check whether dependencies (e.g. `libpulsar.so`) are in the system path. If not, make sure the dependencies are in `LD_LIBRARY_PATH` (on Linux) or `DYLD_LIBRARY_PATH` (on macOS).

Then you can run examples as a simple end-to-end test.

```bash
# In terminal 1
python3 ./examples/consumer.py
```

```bash
# In terminal 2
python3 ./examples/producer.py
```

Before executing the commands above, you must ensure the Pulsar service is running. See [here](https://pulsar.apache.org/docs/getting-started-standalone) for quick start.

## Unit tests

Before running the unit tests, you must run a Pulsar service with all things set up:

```bash
./build-support/pulsar-test-service-start.sh
```

The command above runs a Pulsar standalone in a Docker container. You can run `./build-support/pulsar-test-service-stop.sh` to stop it.

Run all unit tests:

```bash
./tests/run-unit-tests.sh
```

Run a single unit test (e.g. `PulsarTest.test_tls_auth`):

```bash
python3 ./tests/pulsar_test.py 'PulsarTest.test_tls_auth'
```
