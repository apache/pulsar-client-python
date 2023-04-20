#!/usr/bin/env sh
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

cd /

WHEEL=$(ls /pulsar-client-python/wheelhouse/pulsar_client-*.whl)
pip3 install "$WHEEL[avro]"

# Load the wheel to ensure there are no linking problems
python3 -c 'import pulsar; c = pulsar.Client("pulsar://localhost:6650"); c.close()'
python3 -c 'from pulsar.schema import *; s = String(); print(s.schema_info(""));'
