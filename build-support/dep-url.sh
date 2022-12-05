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

pulsar_cpp_base_url() {
  if [[ $# -ne 1 ]]; then
    echo "Usage: pulsar_cpp_base_url <version>"
    exit 1
  fi
  VERSION=$1
  # TODO: use official release url from https://archive.apache.org/
  echo "https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-cpp/pulsar-client-cpp-${VERSION}-candidate-3"
}

download_dependency() {
  if [[ $# -ne 2 ]]; then
    echo "Usage: download_dependency <dependency-name> <dependency-version>"
    exit 1
  fi

  DEP_FILE=$1
  DEP=$2
  # Here we don't use read command to make it available in Alpine
  VERSION=$(grep $DEP $DEP_FILE | sed 's/://' | awk '{print $2}')

  case $DEP in
    "cmake")
      URL=https://github.com/Kitware/CMake/releases/download/v${VERSION}/cmake-${VERSION}-linux-${ARCH}.tar.gz
      ;;
    "pulsar-cpp")
      URL=$(pulsar_cpp_base_url $VERSION)/apache-pulsar-client-cpp-${VERSION}.tar.gz
      ;;
    "pybind11")
      URL=https://github.com/pybind/pybind11/archive/refs/tags/v${VERSION}.tar.gz
      ;;
    "boost")
      VERSION_UNDERSCORE=$(echo $VERSION | sed 's/\./_/g')
      URL=https://boostorg.jfrog.io/artifactory/main/release/${VERSION}/source/boost_${VERSION_UNDERSCORE}.tar.gz
      ;;
    "protobuf")
      URL=https://github.com/google/protobuf/releases/download/v${VERSION}/protobuf-cpp-${VERSION}.tar.gz
      ;;
    "zlib")
      URL=https://github.com/madler/zlib/archive/v${VERSION}.tar.gz
      ;;
    "zstd")
      URL=https://github.com/facebook/zstd/releases/download/v${VERSION}/zstd-${VERSION}.tar.gz
      ;;
    "snappy")
      URL=https://github.com/google/snappy/archive/refs/tags/${VERSION}.tar.gz
      ;;
    "openssl")
      URL=https://github.com/openssl/openssl/archive/OpenSSL_$(echo $VERSION | sed 's/\./_/g').tar.gz
      ;;
    "curl")
      VERSION_UNDERSCORE=$(echo $VERSION | sed 's/\./_/g')
      URL=https://github.com/curl/curl/releases/download/curl-${VERSION_UNDERSCORE}/curl-${VERSION}.tar.gz
      ;;
    *)
      echo "Unknown dependency $DEP for version $VERSION"
      exit 1
  esac
  curl -O -L $URL
  tar zxf $(basename $URL)
}
