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
- [PyBind11](https://github.com/pybind/pybind11)

PyBind11 is a header-only library and a submodule, so you can simply download the submodule so that CMake can find this dependency.

```bash
git submodule update --init
```

You can also download the pybind11 directly like:

```bash
pip3 install pyyaml
export PYBIND11_VERSION=$(./build-support/dep-version.py pybind11)
curl -L -O https://github.com/pybind/pybind11/archive/refs/tags/v${PYBIND11_VERSION}.tar.gz
tar zxf v${PYBIND11_VERSION}.tar.gz
mv pybind11-${PYBIND11_VERSION} pybind11
```

After that, you only need to install the Pulsar C++ client dependency into the system path. You can [install the pre-built binaries](https://pulsar.apache.org/docs/next/client-libraries-cpp/#installation) or [build from source](https://github.com/apache/pulsar-client-cpp#compilation).

## Install the Python wheel

Make sure the PyBind11 submodule has been downloaded and the Pulsar C++ client has been installed. Then run the following commands:

```bash
cmake -B build
cmake --build build 
cmake --install build
python3 ./setup.py bdist_wheel
python3 -m pip install dist/pulsar_client-*.whl --force-reinstall
```

> **NOTE**
>
> 1. Here a separate `build` directory is created to store all CMake temporary files. However, the `setup.py` requires the `_pulsar.so` is under the project directory.
> 2. Add the `--force-reinstall` option to overwrite the existing Python wheel in case your system has already installed a wheel before.
> 3. On Windows, the Python command is `py` instead of `python3`.

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

## Generate API docs

Pulsar Python Client uses [pydoctor](https://github.com/twisted/pydoctor) to generate API docs. To generate by yourself, run the following command in the root path of this repository:

```bash
pip3 install pydoctor
pydoctor --make-html \
  --html-viewsource-base=https://github.com/apache/pulsar-client-python/tree/<release-version-tag> \
  --docformat=numpy --theme=readthedocs \
  --intersphinx=https://docs.python.org/3/objects.inv \
  --html-output=<path-to-apidocs> \
  pulsar
```
