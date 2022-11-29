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

ARG ARCH
FROM quay.io/pypa/manylinux2014_${ARCH}

ARG PYTHON_VERSION
ARG PYTHON_SPEC
ARG ARCH

ENV PYTHON_VERSION=${PYTHON_VERSION}
ENV PYTHON_SPEC=${PYTHON_SPEC}

ENV PATH="/opt/python/${PYTHON_SPEC}/bin:${PATH}"

ENV PYTHON_INCLUDE_DIR /opt/python/${PYTHON_SPEC}/include
ENV PYTHON_LIBRARIES   /opt/python/${PYTHON_SPEC}/lib/python${PYTHON_VERSION}

RUN pip3 install pyyaml

ADD .build/dependencies.yaml /
ADD .build/dep-version.py /usr/local/bin

# Download and install boost
RUN BOOST_VERSION=$(dep-version.py boost) && \
    BOOST_VERSION_UNDESRSCORE=$(echo $BOOST_VERSION | sed 's/\./_/g') && \
    curl -O -L https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz && \
    tar xfz boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz && \
    cp -r boost_${BOOST_VERSION_UNDESRSCORE}/boost /usr/include/ && \
    rm -rf /boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz /boost_${BOOST_VERSION_UNDESRSCORE}

RUN CMAKE_VERSION=$(dep-version.py cmake) && \
    curl -O -L https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${ARCH}.tar.gz && \
    tar xfz cmake-${CMAKE_VERSION}-linux-${ARCH}.tar.gz && \
    cp cmake-${CMAKE_VERSION}-linux-${ARCH}/bin/* /usr/bin/ && \
    cp -r cmake-${CMAKE_VERSION}-linux-${ARCH}/share/cmake-* /usr/share/ && \
    rm -rf cmake-${CMAKE_VERSION}-linux-${ARCH} cmake-${CMAKE_VERSION}-linux-${ARCH}.tar.gz

# Download and compile protobuf
RUN PROTOBUF_VERSION=$(dep-version.py protobuf) && \
    curl -O -L  https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz && \
    tar xfz protobuf-cpp-${PROTOBUF_VERSION}.tar.gz && \
    cd protobuf-${PROTOBUF_VERSION}/ && \
    CXXFLAGS=-fPIC ./configure && \
    make -j8 && make install && ldconfig && \
    rm -rf /protobuf-cpp-${PROTOBUF_VERSION}.tar.gz /protobuf-${PROTOBUF_VERSION}

# ZLib
RUN ZLIB_VERSION=$(dep-version.py zlib) && \
    curl -O -L https://github.com/madler/zlib/archive/v${ZLIB_VERSION}.tar.gz && \
    tar xfz v${ZLIB_VERSION}.tar.gz && \
    cd zlib-${ZLIB_VERSION} && \
    CFLAGS="-fPIC -O3" ./configure && \
    make -j8 && make install && \
    rm -rf /v${ZLIB_VERSION}.tar.gz /zlib-${ZLIB_VERSION}

# Zstandard
RUN ZSTD_VERSION=$(dep-version.py zstd) && \
    curl -O -L https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz && \
    tar xfz zstd-${ZSTD_VERSION}.tar.gz && \
    cd zstd-${ZSTD_VERSION} && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-${ZSTD_VERSION} /zstd-${ZSTD_VERSION}.tar.gz

# Snappy
RUN SNAPPY_VERSION=$(dep-version.py snappy) && \
    curl -O -L https://github.com/google/snappy/archive/refs/tags/${SNAPPY_VERSION}.tar.gz && \
    tar xfz ${SNAPPY_VERSION}.tar.gz && \
    cd snappy-${SNAPPY_VERSION} && \
    CXXFLAGS="-fPIC -O3" cmake . -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF && \
    make -j8 && make install && \
    rm -rf /snappy-${SNAPPY_VERSION} /${SNAPPY_VERSION}.tar.gz

RUN OPENSSL_VERSION=$(dep-version.py openssl) && \
    OPENSSL_VERSION_UNDERSCORE=$(echo $OPENSSL_VERSION | sed 's/\./_/g') && \
    curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz && \
    tar xfz OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz && \
    cd openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}/ && \
    ./config -fPIC --prefix=/usr/local/ssl/ && \
    make -j8 && make install && \
    rm -rf /OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz /openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:/usr/local/lib
ENV OPENSSL_ROOT_DIR /usr/local/ssl/

# LibCurl
RUN CURL_VERSION=$(dep-version.py curl) && \
    CURL_VERSION_UNDERSCORE=$(echo $CURL_VERSION | sed 's/\./_/g') && \
    curl -O -L  https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_UNDERSCORE}/curl-${CURL_VERSION}.tar.gz && \
    tar xfz curl-${CURL_VERSION}.tar.gz && \
    cd curl-${CURL_VERSION} && \
    CFLAGS=-fPIC ./configure --with-ssl=/usr/local/ssl/ --without-zstd && \
    make -j8 && make install && \
    rm -rf /curl-${CURL_VERSION}.tar.gz /curl-${CURL_VERSION}

# Pulsar client C++
RUN PULSAR_CPP_VERSION=$(dep-version.py pulsar-cpp) && \
    curl -O -L https://archive.apache.org/dist/pulsar/pulsar-client-cpp-${PULSAR_CPP_VERSION}/apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}.tar.gz && \
    tar xfz apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}.tar.gz && \
    cd apache-pulsar-client-cpp-${PULSAR_CPP_VERSION} && \
    cmake . -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_STATIC_LIB=OFF -DLINK_STATIC=ON && \
    make -j8 && \
    make install && \
    rm -rf apache-pulsar-client-cpp-${PULSAR_CPP_VERSION} apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}.tar.gz
