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

PYTHON_VERSION=$1
PYTHON_VERSION_LONG=$2

source pkg/mac/common.sh

pip3 install pyyaml

dep=$ROOT_DIR/build-support/dep-version.py
ZLIB_VERSION=$($dep zlib)
OPENSSL_VERSION=$($dep openssl)
BOOST_VERSION=$($dep boost)
PROTOBUF_VERSION=$($dep protobuf)
ZSTD_VERSION=$($dep zstd)
SNAPPY_VERSION=$($dep snappy)
CURL_VERSION=$($dep curl)

# Compile and cache dependencies
CACHE_DIR=${CACHE_DIR_DEPS}
mkdir -p $CACHE_DIR
cd $CACHE_DIR

PREFIX=$CACHE_DIR/install


###############################################################################
if [ ! -f zlib-${ZLIB_VERSION}/.done ]; then
    echo "Building ZLib"
    curl -O -L https://zlib.net/zlib-${ZLIB_VERSION}.tar.gz
    tar xfz zlib-$ZLIB_VERSION.tar.gz
    pushd zlib-$ZLIB_VERSION
      CFLAGS="-fPIC -O3 -arch arm64 -arch x86_64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" ./configure --prefix=$PREFIX
      make -j16
      make install
      touch .done
    popd
else
    echo "Using cached ZLib"
fi

###############################################################################
if [ ! -f Python-${PYTHON_VERSION_LONG}/.done ]; then
  echo "Building Python $PYTHON_VERSION_LONG"
  curl -O -L https://www.python.org/ftp/python/${PYTHON_VERSION_LONG}/Python-${PYTHON_VERSION_LONG}.tgz
  tar xfz Python-${PYTHON_VERSION_LONG}.tgz

  pushd Python-${PYTHON_VERSION_LONG}
      if [ $PYTHON_VERSION = '3.7' ]; then
          UNIVERSAL_ARCHS='intel-64'
          PY_CFLAGS=" -arch x86_64"
      else
          UNIVERSAL_ARCHS='universal2'
      fi

      CFLAGS="-fPIC -O3 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET} -I${PREFIX}/include ${PY_CFLAGS}" \
          LDFLAGS=" ${PY_CFLAGS} -L${PREFIX}/lib" \
          ./configure --prefix=$PREFIX --enable-shared --enable-universalsdk --with-universal-archs=${UNIVERSAL_ARCHS}
      make -j16
      make install

      curl -O -L https://files.pythonhosted.org/packages/27/d6/003e593296a85fd6ed616ed962795b2f87709c3eee2bca4f6d0fe55c6d00/wheel-0.37.1-py2.py3-none-any.whl
      $PREFIX/bin/pip3 install wheel-*.whl

      touch .done
  popd
else
  echo "Using cached Python $PYTHON_VERSION_LONG"
fi

###############################################################################
OPENSSL_VERSION_UNDERSCORE=$(echo $OPENSSL_VERSION | sed 's/\./_/g')
if [ ! -f openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.done ]; then
    echo "Building OpenSSL"
    curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz
    # -arch arm64 -arch x86_64
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

###############################################################################
BOOST_VERSION_=${BOOST_VERSION//./_}
DIR=boost-src-${BOOST_VERSION}
if [ ! -f $DIR/.done ]; then
    echo "Building Boost for Py $PYTHON_VERSION"
    curl -O -L https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_}.tar.gz
    tar xfz boost_${BOOST_VERSION_}.tar.gz
    mv boost_${BOOST_VERSION_} $DIR

    PY_INCLUDE_DIR=${PREFIX}/include/python${PYTHON_VERSION}
    if [ $PYTHON_VERSION = '3.7' ]; then
        PY_INCLUDE_DIR=${PY_INCLUDE_DIR}m
    fi

    pushd $DIR
      cat <<EOF > user-config.jam
        using python : $PYTHON_VERSION
                : python3
                : ${PY_INCLUDE_DIR}
                : ${PREFIX}/lib
              ;
EOF
      ./bootstrap.sh --with-libraries=python --with-python=python3 --with-python-root=$PREFIX \
            --prefix=${PREFIX}
      ./b2 -d0 address-model=64 cxxflags="-fPIC -arch arm64 -arch x86_64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
                link=static threading=multi \
                --user-config=./user-config.jam \
                variant=release python=${PYTHON_VERSION} \
                -j16 \
                install
      touch .done
    popd
else
    echo "Using cached Boost for Py $PYTHON_VERSION"
fi



###############################################################################
if [ ! -f protobuf-${PROTOBUF_VERSION}/.done ]; then
    echo "Building Protobuf"
    curl -O -L  https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz
    tar xfz protobuf-cpp-${PROTOBUF_VERSION}.tar.gz
    pushd protobuf-${PROTOBUF_VERSION}
      CXXFLAGS="-fPIC -arch arm64 -arch x86_64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
            ./configure --prefix=$PREFIX
      make -j16
      make install
      touch .done
    popd
else
    echo "Using cached Protobuf"
fi

###############################################################################
if [ ! -f zstd-${ZSTD_VERSION}/.done ]; then
    echo "Building ZStd"
    curl -O -L https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz
    tar xfz zstd-${ZSTD_VERSION}.tar.gz
    pushd zstd-${ZSTD_VERSION}
      CFLAGS="-fPIC -O3 -arch arm64 -arch x86_64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" PREFIX=$PREFIX \
            make -j16 -C lib install-static install-includes
      touch .done
    popd
else
    echo "Using cached ZStd"
fi

###############################################################################
if [ ! -f snappy-${SNAPPY_VERSION}/.done ]; then
    echo "Building Snappy"
    curl -O -L https://github.com/google/snappy/archive/refs/tags/${SNAPPY_VERSION}.tar.gz
    tar xfz ${SNAPPY_VERSION}.tar.gz
    pushd snappy-${SNAPPY_VERSION}
      CXXFLAGS="-fPIC -O3 -arch arm64 -arch x86_64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
          cmake . -DCMAKE_INSTALL_PREFIX=$PREFIX -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF
      make -j16
      make install
      touch .done
    popd
else
    echo "Using cached Snappy"
fi

###############################################################################
if [ ! -f curl-${CURL_VERSION}/.done ]; then
    echo "Building LibCurl"
    CURL_VERSION_=${CURL_VERSION//./_}
    curl -O -L  https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_}/curl-${CURL_VERSION}.tar.gz
    tar xfz curl-${CURL_VERSION}.tar.gz
    pushd curl-${CURL_VERSION}
      CFLAGS="-fPIC -arch arm64 -arch x86_64 -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}" \
            ./configure --with-ssl=$PREFIX \
              --without-nghttp2 \
              --without-libidn2 \
              --disable-ldap \
              --without-brotli \
              --without-secure-transport \
              --disable-ipv6 \
              --prefix=$PREFIX
      make -j16 install
      touch .done
    popd
else
    echo "Using cached LibCurl"
fi
