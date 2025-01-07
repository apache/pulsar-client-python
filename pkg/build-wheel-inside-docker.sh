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

# Build cpp wheels
if [[ $ARCH == "aarch64" ]]; then
    export VCPKG_FORCE_SYSTEM_BINARIES=1
fi
PULSAR_CPP_VERSION=$(dep-version.py pulsar-cpp)
. /dep-url.sh
download_dependency /dependencies.yaml pulsar-cpp
cd apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}

git clone https://github.com/microsoft/vcpkg.git
cd vcpkg
# The following dependencies are required to build vcpkg on arm64 Linux
yum install -y curl zip unzip tar perl-IPC-Cmd
git clone https://github.com/ninja-build/ninja.git
cd ninja
git checkout release
/opt/python/cp312-cp312/bin/python configure.py --bootstrap
mv ninja /usr/bin/
cd ..
./bootstrap-vcpkg.sh
cd ..

cmake -B build -DINTEGRATE_VCPKG=ON -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_DYNAMIC_LIB=ON -DBUILD_STATIC_LIB=ON
cmake --build build -j8 --target install
cd ..
rm -rf apache-pulsar-client-cpp-$(PULSAR_CPP_VERSION)

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
