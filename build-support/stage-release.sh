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

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 \$VERSION \$WORKFLOW_ID"
    exit 1
fi
if [[ ! $GITHUB_TOKEN ]]; then
    echo "GITHUB_TOKEN (must have the actions scope) is not set"
    exit 2
fi

VERSION=$1
TAG=v$VERSION
WORKFLOW_ID=$2

# Download the source tar
curl -O -L https://github.com/apache/pulsar-client-python/archive/refs/tags/$TAG.tar.gz

# Remove the "-candidate-N" suffix
VERSION=$(echo $VERSION | sed 's/-candidate-.*//')
mv $TAG.tar.gz pulsar-client-python-$VERSION.tar.gz

# Download the Python wheels
URLS=$(curl -L https://api.github.com/repos/apache/pulsar-client-python/actions/runs/$WORKFLOW_ID/artifacts \
  | jq '.artifacts[] .archive_download_url' | sed 's/^"\(.*\)"$/\1/')
for URL in $URLS; do
    curl -O -L $URL -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $GITHUB_TOKEN"
    unzip -q zip
    rm -f zip
done

sign() {
    FILE=$1
    gpg --armor --output $FILE.asc --detach-sig $FILE
    shasum -a 512 $FILE > $FILE.sha512
}

export GPG_TTY=$(tty)
set -x
for WHEEL in $(ls *.whl); do
    sign $WHEEL
done
sign pulsar-client-python-$VERSION.tar.gz

mkdir windows && cd windows
mv ../pulsar_client*win*.whl* . && cd ..

mkdir macos && cd macos
mv ../pulsar_client*macos*.whl* . && cd ..

mkdir linux-glibc-x86_64 && cd linux-glibc-x86_64
mv ../pulsar_client*manylinux*x86_64.whl* . && cd ..

mkdir linux-glibc-arm64 && cd linux-glibc-arm64
mv ../pulsar_client*manylinux*aarch64.whl* . && cd ..

mkdir linux-musl-x86_64 && cd linux-musl-x86_64
mv ../pulsar_client*musllinux*x86_64.whl* . && cd ..

mkdir linux-musl-arm64 && cd linux-musl-arm64
mv ../pulsar_client*musllinux*aarch64.whl* . && cd ..
