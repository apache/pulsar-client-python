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

cd `dirname $0`

CPP_CLIENT_VERSION=$(./dep-version.py pulsar-cpp ../dependencies.yaml)
PYBIND11_VERSION=$(./dep-version.py pybind11 ../dependencies.yaml)
source ./dep-url.sh

if [ $USER != "root" ]; then
  SUDO="sudo"
fi

# Get the flavor of Linux
export $(cat /etc/*-release | grep "^ID=")

cd /tmp

# Fetch the client binaries
BASE_URL=$(pulsar_cpp_base_url ${CPP_CLIENT_VERSION})

UNAME_ARCH=$(uname -m)
if [ $UNAME_ARCH == 'aarch64' ]; then
  PLATFORM=arm64
else
  PLATFORM=x86_64
fi

if [ $ID == 'ubuntu' ]; then
  curl -L -O ${BASE_URL}/deb-${PLATFORM}/apache-pulsar-client.deb
  curl -L -O ${BASE_URL}/deb-${PLATFORM}/apache-pulsar-client-dev.deb
  $SUDO apt install -y /tmp/*.deb

elif [ $ID == 'alpine' ]; then
  curl -L -O ${BASE_URL}/apk-${PLATFORM}/${UNAME_ARCH}/apache-pulsar-client-${CPP_CLIENT_VERSION}-r0.apk
  curl -L -O ${BASE_URL}/apk-${PLATFORM}/${UNAME_ARCH}/apache-pulsar-client-dev-${CPP_CLIENT_VERSION}-r0.apk
  $SUDO apk add --allow-untrusted /tmp/*.apk

elif [ $ID == '"centos"' ]; then
  curl -L -O ${BASE_URL}/rpm-${PLATFORM}/${UNAME_ARCH}/apache-pulsar-client-${CPP_CLIENT_VERSION}-1.${UNAME_ARCH}.rpm
  curl -L -O ${BASE_URL}/rpm-${PLATFORM}/${UNAME_ARCH}/apache-pulsar-client-devel-${CPP_CLIENT_VERSION}-1.${UNAME_ARCH}.rpm
  $SUDO rpm -i /tmp/*.rpm

else
  echo "Unknown Linux distribution: '$ID'"
  exit 1
fi

curl -L -O https://github.com/pybind/pybind11/archive/refs/tags/v${PYBIND11_VERSION}.tar.gz
tar zxf v${PYBIND11_VERSION}.tar.gz
$SUDO cp -rf pybind11-${PYBIND11_VERSION}/include/pybind11 /usr/include/
rm -rf pybind11-${PYBIND11_VERSION} v${PYBIND11_VERSION}.tar.gz
