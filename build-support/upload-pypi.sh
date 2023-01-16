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

set -e

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 BASE_URL"
    exit 1
fi

BASE_URL=$1

download_wheels() {
    URL=$BASE_URL/$1
    LIST=$(curl -L $URL | grep ".whl<" | sed 's/^.*>pulsar\(.*\)\.whl.*/pulsar\1.whl/')
    for WHEEL in $LIST; do
        if [[ ! -f $WHEEL ]]; then
            echo "Download $URL/$WHEEL"
            curl -O -L $URL/$WHEEL
        fi
    done
}

download_wheels linux-glibc-arm64
download_wheels linux-glibc-x86_64
download_wheels linux-musl-arm64
download_wheels linux-musl-x86_64
download_wheels macos
download_wheels windows

twine upload pulsar_client-*.whl
rm -f pulsar_client-*.whl
