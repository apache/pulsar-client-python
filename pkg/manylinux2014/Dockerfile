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
ADD .build/dep-url.sh /

# Download and install boost
RUN BOOST_VERSION=$(dep-version.py boost) && \
    BOOST_VERSION_UNDESRSCORE=$(echo $BOOST_VERSION | sed 's/\./_/g') && \
    . /dep-url.sh && download_dependency /dependencies.yaml boost && \
    cp -r boost_${BOOST_VERSION_UNDESRSCORE}/boost /usr/include/ && \
    rm -rf /boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz /boost_${BOOST_VERSION_UNDESRSCORE}

RUN CMAKE_VERSION=$(dep-version.py cmake) && \
    . /dep-url.sh && ARCH=${ARCH} download_dependency /dependencies.yaml cmake && \
    cp cmake-${CMAKE_VERSION}-linux-${ARCH}/bin/* /usr/bin/ && \
    cp -r cmake-${CMAKE_VERSION}-linux-${ARCH}/share/cmake-* /usr/share/ && \
    rm -rf cmake-${CMAKE_VERSION}-linux-${ARCH} cmake-${CMAKE_VERSION}-linux-${ARCH}.tar.gz

# Download and compile protobuf
RUN PROTOBUF_VERSION=$(dep-version.py protobuf) && \
    . /dep-url.sh && download_dependency /dependencies.yaml protobuf && \
    cd protobuf-${PROTOBUF_VERSION}/ && \
    CXXFLAGS=-fPIC ./configure && \
    make -j8 && make install && ldconfig && \
    rm -rf /protobuf-cpp-${PROTOBUF_VERSION}.tar.gz /protobuf-${PROTOBUF_VERSION}

# ZLib
RUN ZLIB_VERSION=$(dep-version.py zlib) && \
    . /dep-url.sh && download_dependency /dependencies.yaml zlib && \
    cd zlib-${ZLIB_VERSION} && \
    CFLAGS="-fPIC -O3" ./configure && \
    make -j8 && make install && \
    rm -rf /v${ZLIB_VERSION}.tar.gz /zlib-${ZLIB_VERSION}

# Zstandard
RUN ZSTD_VERSION=$(dep-version.py zstd) && \
    . /dep-url.sh && download_dependency /dependencies.yaml zstd && \
    cd zstd-${ZSTD_VERSION} && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-${ZSTD_VERSION} /zstd-${ZSTD_VERSION}.tar.gz

# Snappy
RUN SNAPPY_VERSION=$(dep-version.py snappy) && \
    . /dep-url.sh && download_dependency /dependencies.yaml snappy && \
    cd snappy-${SNAPPY_VERSION} && \
    CXXFLAGS="-fPIC -O3" cmake . -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF && \
    make -j8 && make install && \
    rm -rf /snappy-${SNAPPY_VERSION} /${SNAPPY_VERSION}.tar.gz

RUN OPENSSL_VERSION=$(dep-version.py openssl) && \
    OPENSSL_VERSION_UNDERSCORE=$(echo $OPENSSL_VERSION | sed 's/\./_/g') && \
    . /dep-url.sh && download_dependency /dependencies.yaml openssl && \
    cd openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}/ && \
    ./config -fPIC --prefix=/usr/local/ssl/ && \
    make -j8 && make install && \
    rm -rf /OpenSSL_${OPENSSL_VERSION_UNDERSCORE}.tar.gz /openssl-OpenSSL_${OPENSSL_VERSION_UNDERSCORE}

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:/usr/local/lib
ENV OPENSSL_ROOT_DIR /usr/local/ssl/

# LibCurl
RUN CURL_VERSION=$(dep-version.py curl) && \
    . /dep-url.sh && download_dependency /dependencies.yaml curl && \
    cd curl-${CURL_VERSION} && \
    CFLAGS=-fPIC ./configure --with-ssl=/usr/local/ssl/ --without-zstd && \
    make -j8 && make install && \
    rm -rf /curl-${CURL_VERSION}.tar.gz /curl-${CURL_VERSION}

# Pulsar client C++
RUN PULSAR_CPP_VERSION=$(dep-version.py pulsar-cpp) && \
    . /dep-url.sh && download_dependency /dependencies.yaml pulsar-cpp && \
    cd apache-pulsar-client-cpp-${PULSAR_CPP_VERSION} && \
    cmake . -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_STATIC_LIB=OFF -DLINK_STATIC=ON && \
    make -j8 && \
    make install && \
    rm -rf apache-pulsar-client-cpp-${PULSAR_CPP_VERSION} apache-pulsar-client-cpp-${PULSAR_CPP_VERSION}.tar.gz
