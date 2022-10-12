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

CACHE_DIR=~/.pulsar-python-deps
if [[ $# -le 1 ]]; then
    CACHE_DIR=$(mkdir -p $1 && cd $1 && pwd)
fi
PREFIX=$CACHE_DIR/install
echo "Use $CACHE_DIR to cache dependencies"

python3 -m pip install pyyaml

BOOST_VERSION=$(./build-support/dep-version.py boost)
mkdir -p $CACHE_DIR
cd $CACHE_DIR

download() {
    URL=$1
    BASENAME=$(basename $URL)
    if [[ ! -f $BASENAME ]]; then
        echo "curl -O -L $URL"
        curl -O -L $URL
    fi
    tar xfz $BASENAME
}

# Install Boost
BOOST_VERSION_=${BOOST_VERSION//./_}
DIR=boost_$BOOST_VERSION_
if [[ ! -f $DIR/.done ]]; then
    echo "Building Boost $BOOST_VERSION"
    download https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_}.tar.gz
    mkdir -p $PREFIX/include
    pushd $DIR
        ./bootstrap.sh --with-libraries=python --with-python=python3 --prefix=$PREFIX 2>&1 >/dev/null
        ./b2 address-model=64 cxxflags="-fPIC" link=static threading=multi -j8 install 2>&1 >/dev/null
        touch .done
    popd
else
    echo "Using cached Boost $BOOST_VERSION"
fi
