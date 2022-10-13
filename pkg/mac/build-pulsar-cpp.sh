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

ROOT_DIR=$(git rev-parse --show-toplevel)
cd "${ROOT_DIR}"

source pkg/mac/common.sh

PULSAR_CPP_VERSION=$(cat pulsar-client-cpp-version.txt | xargs)

# Compile and cache dependencies
mkdir -p $CACHE_DIR_CPP_CLIENT
cd $CACHE_DIR_CPP_CLIENT

PREFIX=$CACHE_DIR_CPP_CLIENT/install

DEPS_PREFIX=${CACHE_DIR_DEPS}/install

###############################################################################

## TODO: Fetch from official release
curl -O -L https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-cpp-${PULSAR_CPP_VERSION}-candidate-1/apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}.tar.gz
tar xfz apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}.tar.gz

if [ ! -f apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}/.done ]; then
  pushd apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}
      ARCHS='arm64;x86_64'

      cmake . \
              -DCMAKE_OSX_ARCHITECTURES=${ARCHS} \
              -DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET} \
              -DCMAKE_INSTALL_PREFIX=$PREFIX \
              -DCMAKE_BUILD_TYPE=Release \
              -DCMAKE_PREFIX_PATH=${DEPS_PREFIX} \
              -DCMAKE_CXX_FLAGS=-I${DEPS_PREFIX}/include \
              -DLINK_STATIC=OFF \
              -DBUILD_TESTS=OFF \
              -DBUILD_WIRESHARK=OFF \
              -DBUILD_DYNAMIC_LIB=OFF \
              -DBUILD_STATIC_LIB=ON \
              -DPROTOC_PATH=${DEPS_PREFIX}/bin/protoc

      make -j16 install
  popd
fi
