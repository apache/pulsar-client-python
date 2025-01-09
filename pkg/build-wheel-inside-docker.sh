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
source build-support/dep-url.sh

# Build cpp wheels
PULSAR_CPP_VERSION=$(cat ./dependencies.yaml | grep pulsar-cpp | awk '{print $2}')

if [ $CPP_BINARY_TYPE == "rpm" ]; then
    if [ $ARCH == "aarch64" ]; then
        RPM_ROOT_DIR=$(pulsar_cpp_base_url $PULSAR_CPP_VERSION)/rpm-arm64/aarch64
    else
        RPM_ROOT_DIR=$(pulsar_cpp_base_url $PULSAR_CPP_VERSION)/rpm-x86_64/x86_64
    fi
    curl -O -L $RPM_ROOT_DIR/apache-pulsar-client-$PULSAR_CPP_VERSION-1.$ARCH.rpm
    curl -O -L $RPM_ROOT_DIR/apache-pulsar-client-devel-$PULSAR_CPP_VERSION-1.$ARCH.rpm
    curl -O -L $RPM_ROOT_DIR/apache-pulsar-client-debuginfo-$PULSAR_CPP_VERSION-1.$ARCH.rpm
    rpm -ivh *.rpm
else # apk
    if [ $ARCH == "aarch64" ]; then
        APK_ROOT_DIR=$(pulsar_cpp_base_url $PULSAR_CPP_VERSION)/apk-arm64/aarch64
    else
        APK_ROOT_DIR=$(pulsar_cpp_base_url $PULSAR_CPP_VERSION)/apk-x86_64/x86_64
    fi
    curl -O -L $APK_ROOT_DIR/apache-pulsar-client-$PULSAR_CPP_VERSION-r0.apk
    curl -O -L $APK_ROOT_DIR/apache-pulsar-client-dev-$PULSAR_CPP_VERSION-r0.apk
    apk add --allow-untrusted *.apk
fi

download_dependency $PWD/dependencies.yaml pybind11
rm -rf pybind11
mv pybind11-* pybind11

cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j8
mv build/lib_pulsar.so .

./setup.py bdist_wheel

# Audit wheel is required to convert a wheel that is tagged as generic
# 'linux' into a 'multilinux' wheel.
# Only wheel files tagged as multilinux can be uploaded to PyPI
# Audit wheel will make sure no external dependencies are needed for
# the shared library and that only symbols supported by most linux
# distributions are used.
auditwheel repair dist/pulsar_client*-$PYTHON_SPEC-linux_*.whl
