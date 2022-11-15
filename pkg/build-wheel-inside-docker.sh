#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e -x

cd /pulsar-client-python

PYBIND11_VERSION=$(./build-support/dep-version.py pybind11)
curl -L -O https://github.com/pybind/pybind11/archive/refs/tags/v${PYBIND11_VERSION}.tar.gz
tar zxf v${PYBIND11_VERSION}.tar.gz
rm -rf pybind11
mv pybind11-${PYBIND11_VERSION} pybind11

rm -f CMakeCache.txt CMakeFiles

cmake . \
      -DCMAKE_BUILD_TYPE=Release

make -j4

./setup.py bdist_wheel

# Audit wheel is required to convert a wheel that is tagged as generic
# 'linux' into a 'multilinux' wheel.
# Only wheel files tagged as multilinux can be uploaded to PyPI
# Audit wheel will make sure no external dependencies are needed for
# the shared library and that only symbols supported by most linux
# distributions are used.
auditwheel repair dist/pulsar_client*-$PYTHON_SPEC-linux_*.whl
