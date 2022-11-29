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

PYTHON_CLIENT_VERSION=$(cat version.txt | xargs)

PYTHON_VERSION=$1

###############################################################################
###############################################################################
###############################################################################
###############################################################################
PREFIX=${CACHE_DIR_DEPS}/install

echo '----------------------------------------------------------------------------'
echo '----------------------------------------------------------------------------'
echo '----------------------------------------------------------------------------'
echo "Build wheel for Python $PYTHON_VERSION"

cd "${ROOT_DIR}"

rm -f CMakeCache.txt

PY_EXE=$PREFIX/bin/python3
PIP_EXE=$PREFIX/bin/pip3

ARCHS='arm64;x86_64'
PIP_TAG='universal2'

cmake . \
        -DCMAKE_OSX_ARCHITECTURES=${ARCHS} \
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET} \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH=$PREFIX \
        -DCMAKE_CXX_FLAGS=-I$PREFIX/include \
        -DLINK_STATIC=ON \
        -DPULSAR_LIBRARY=${CACHE_DIR_CPP_CLIENT}/install/lib/libpulsar.a \
        -DPULSAR_INCLUDE=${CACHE_DIR_CPP_CLIENT}/install/include \
        -DPython3_ROOT_DIR=$PREFIX

make clean
make -j16

$PY_EXE setup.py bdist_wheel

PY_SPEC=$(echo $PYTHON_VERSION | sed 's/\.//g')

cd /tmp
$PIP_EXE install --no-dependencies --force-reinstall \
    $ROOT_DIR/dist/pulsar_client-${PYTHON_CLIENT_VERSION}-cp$PY_SPEC-*-macosx_10_15_${PIP_TAG}.whl
$PY_EXE -c 'import pulsar'

