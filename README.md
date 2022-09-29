<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pulsar Python client library
<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

## How to build it from source

Requirements:
- C++ compiler that supports C++11
- CMake >= 3.12
- Boost.Python

There are some other library dependencies (i.e. CMake is able to find the libraries), see the following list:
- [libcurl](https://curl.se/libcurl/)
- [OpenSSL](https://www.openssl.org/)
- [Protocol Buffers](https://github.com/protocolbuffers/protobuf) >= 3
- [zlib](https://github.com/madler/zlib)
- [zstd](https://github.com/facebook/zstd)
- [snappy](https://github.com/google/snappy)

The Python client relies on the C++ client, so we must build the C++ client first.

```bash
./build-client-cpp.sh
```

> You can set the `CMAKE_CPP_OPTIONS` environment variable to add your customized CMake options when building the C++ client.

After the C++ client is built successfully, you can build the Python library with CMake.

```bash
cmake -B build
cmake --build build
```

Then we will have `_pulsar.so` under the `build` directory.

To verify it works, you should copy `_pulsar.so` into the project directory. Then run `python3 -c 'import pulsar'` to verify it.
