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
FROM quay.io/pypa/musllinux_1_1_${ARCH}

ARG PYTHON_VERSION
ARG PYTHON_SPEC

ENV PYTHON_VERSION=${PYTHON_VERSION}
ENV PYTHON_SPEC=${PYTHON_SPEC}

ARG ARCH
ENV ARCH=${ARCH}

ENV PATH="/opt/python/${PYTHON_SPEC}/bin:${PATH}"
ENV PYTHON_INCLUDE_DIR /opt/python/${PYTHON_SPEC}/include
ENV PYTHON_LIBRARIES   /opt/python/${PYTHON_SPEC}/lib/python${PYTHON_VERSION}

RUN pip install pyyaml

RUN apk add cmake

ADD .build/dependencies.yaml /
ADD .build/dep-version.py /usr/local/bin

# Download and compile boost
RUN BOOST_VERSION=$(dep-version.py boost) && \
    BOOST_VERSION_UNDESRSCORE=$(echo $BOOST_VERSION | sed 's/\./_/g') && \
    curl -O -L https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz && \
    tar xfz boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz && \
    cd boost_${BOOST_VERSION_UNDESRSCORE} && \
    ./bootstrap.sh --with-libraries=python && \
    ./b2 -d0  address-model=64 cxxflags=-fPIC link=shared threading=multi variant=release install && \
    rm -rf /boost_${BOOST_VERSION_UNDESRSCORE}.tar.gz /boost_${BOOST_VERSION_UNDESRSCORE}


