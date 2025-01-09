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

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <python-version> <python-version-long>"
    exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
cd "${ROOT_DIR}"

source build-support/dep-url.sh

CACHE_DIR=$ROOT_DIR/.pulsar-mac-build
PREFIX=${CACHE_DIR}/install
mkdir -p $PREFIX

mkdir -p $PREFIX/lib/
if [ ! -f $PREFIX/lib/libpulsarwithdeps.a ]; then
    VERSION=$(cat ./dependencies.yaml | grep pulsar-cpp | awk '{print $2}')
    curl -O -L $(pulsar_cpp_base_url $VERSION)/macos-arm64.zip
    curl -O -L $(pulsar_cpp_base_url $VERSION)/macos-x86_64.zip

    unzip -q macos-arm64.zip -d arm64
    unzip -q macos-x86_64.zip -d x86_64
    libtool -static -o libpulsarwithdeps.a arm64/lib/libpulsarwithdeps.a x86_64/lib/libpulsarwithdeps.a

    mv arm64/include/ $PREFIX/
    mv libpulsarwithdeps.a $PREFIX/lib/
    rm -rf arm64/ x86_64/ macos-arm64.zip macos-x86_64.zip
fi

PYTHON_VERSION=$1
PYTHON_VERSION_LONG=$2

MACOSX_DEPLOYMENT_TARGET=13
pushd $CACHE_DIR

# We need to build OpenSSL from source to have universal2 binaries
OPENSSL_VERSION=$(cat $ROOT_DIR/dependencies.yaml | grep openssl | awk '{print $2}')
OPENSSL_VERSION_UNDERSCORE=$(echo $OPENSSL_VERSION | sed 's/\./_/g')
if [ ! -f openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.done ]; then
    echo "Building OpenSSL"
    download_dependency $ROOT_DIR/dependencies.yaml openssl
    tar xfz OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz

    mv openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE} openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-arm64
    pushd openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-arm64
      echo -e "#include <string.h>\n$(cat test/v3ext.c)" > test/v3ext.c
      CFLAGS="-fPIC -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
          ./Configure --prefix=$PREFIX no-shared no-unit-test darwin64-arm64-cc
      make -j8
      make install_sw
    popd

    tar xfz OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz
    mv openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE} openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-x86_64
    pushd openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-x86_64
    echo -e "#include <string.h>\n$(cat test/v3ext.c)" > test/v3ext.c
      CFLAGS="-fPIC -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
          ./Configure --prefix=$PREFIX no-shared no-unit-test darwin64-x86_64-cc
      make -j8
      make install_sw
    popd

    # Create universal binaries
    lipo -create openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-arm64/libssl.a openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-x86_64/libssl.a \
          -output $PREFIX/lib/libssl.a
    lipo -create openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-arm64/libcrypto.a openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}-x86_64/libcrypto.a \
              -output $PREFIX/lib/libcrypto.a

    touch openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.done
else
    echo "Using cached OpenSSL"
fi

if [ ! -f Python-${PYTHON_VERSION_LONG}/.done ]; then
    echo "Building Python $PYTHON_VERSION_LONG"
    curl -O -L https://www.python.org/ftp/python/${PYTHON_VERSION_LONG}/Python-${PYTHON_VERSION_LONG}.tgz
    tar xfz Python-${PYTHON_VERSION_LONG}.tgz

    pushd Python-${PYTHON_VERSION_LONG}
        ./configure --prefix=$PREFIX --enable-shared --enable-universalsdk --with-universal-archs=universal2 --with-openssl=$PREFIX
        make -j16
        make install

        curl -O -L https://files.pythonhosted.org/packages/27/d6/003e593296a85fd6ed616ed962795b2f87709c3eee2bca4f6d0fe55c6d00/wheel-0.37.1-py2.py3-none-any.whl
        export SSL_CERT_FILE=/etc/ssl/cert.pem
        $PREFIX/bin/pip3 install wheel setuptools
        $PREFIX/bin/pip3 install wheel-*.whl

        touch .done
    popd
else
    echo "Using cached Python $PYTHON_VERSION_LONG"
fi

PYBIND11_VERSION=$(cat $ROOT_DIR/dependencies.yaml | grep pybind11 | awk '{print $2}')
if [ ! -f pybind11/.done ]; then
    download_dependency $ROOT_DIR/dependencies.yaml pybind11
    mkdir -p $PREFIX/include/
    cp -rf pybind11-${PYBIND11_VERSION}/include/pybind11 $PREFIX/include/
    mkdir -p pybind11
    touch pybind11/.done
fi

popd # $CACHE_DIR

PYTHON_CLIENT_VERSION=$(grep -v '^#' pulsar/__about__.py | cut -d "=" -f2 | sed "s/'//g")

echo "Build wheel for Python $PYTHON_VERSION"

PY_EXE=$PREFIX/bin/python3
PIP_EXE=$PREFIX/bin/pip3

ARCHS='arm64;x86_64'
PIP_TAG='universal2'

cmake -B build \
        -DCMAKE_OSX_ARCHITECTURES=${ARCHS} \
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET} \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH=$PREFIX \
        -DLINK_STATIC=ON \
        -DPython3_ROOT_DIR=$PREFIX
cmake --build build --config Release -j8
cp -f build/lib_pulsar.so .

$PY_EXE setup.py bdist_wheel

PY_SPEC=$(echo $PYTHON_VERSION | sed 's/\.//g')

cd /tmp
$PIP_EXE install --no-dependencies --force-reinstall \
    $ROOT_DIR/dist/pulsar_client-${PYTHON_CLIENT_VERSION}-cp$PY_SPEC-*-macosx*_${PIP_TAG}.whl
$PY_EXE -c 'import pulsar'
