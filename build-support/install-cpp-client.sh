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

cd $ROOT_DIR

CPP_CLIENT_VERSION=$(cat pulsar-client-cpp-version.txt | xargs)

# Fetch the client binaries
## TODO: Fetch from official release once it's available
pushd /tmp
  curl -L -O https://github.com/merlimat/pulsar-client-cpp/releases/download/${CPP_CLIENT_VERSION}/apache-pulsar-client.deb
  curl -L -O https://github.com/merlimat/pulsar-client-cpp/releases/download/${CPP_CLIENT_VERSION}/apache-pulsar-client-dev.deb
popd

sudo apt install /tmp/apache-pulsar-client.deb /tmp/apache-pulsar-client-dev.deb
