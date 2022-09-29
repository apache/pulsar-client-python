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

project (pulsar-client-python)
cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_PREFIX_PATH
    ${CMAKE_PREFIX_PATH}
    ${CMAKE_SOURCE_DIR}/pulsar-client-cpp/build)

set(PULSAR_CPP_INCLUDE_DIRS
    ${CMAKE_SOURCE_DIR}/pulsar-client-cpp/include
    ${CMAKE_SOURCE_DIR}
)

# Enable this option if the Pulsar C++ client was built with LINK_STATIC=ON
option(PULASR_CPPLINK_STATIC "Link against static libraries" OFF)

if (NOT PULASR_CPPLINK_STATIC)
    set(OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR} /usr/lib64/)
    ### This part is to find and keep SSL dynamic libs in RECORD_OPENSSL_SSL_LIBRARY and RECORD_OPENSSL_CRYPTO_LIBRARY
    ### After find the libs, will unset related cache, and will not affect another same call to find_package.
    if (APPLE)
        set(OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR} /usr/local/opt/openssl/ /opt/homebrew/opt/openssl)
    endif ()
    set(OPENSSL_USE_STATIC_LIBS TRUE)

    find_package(OpenSSL REQUIRED)
    find_library(CURL_LIBRARIES NAMES libcurl.a curl curl_a libcurl_a)
    find_library(Protobuf_LIBRARIES NAMES libprotobuf.a libprotobuf)
    find_library(ZLIB_LIBRARIES REQUIRED NAMES libz.a z zlib)
    find_library(ZSTD_LIBRARIES NAMES libzstd.a)
    find_library(SNAPPY_LIBRARIES NAMES libsnappy.a)

    set(COMMON_LIBS
        ${OPENSSL_CRYPTO_LIBRARY}
        ${CURL_LIBRARIES}
        ${OPENSSL_SSL_LIBRARY}
        ${Protobuf_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${ZSTD_LIBRARIES}
        ${SNAPPY_LIBRARIES}
    )
    message(STATUS "COMMON_LIBS: " ${COMMON_LIBS})
endif ()

find_library(PULSAR_CPP_LIBRARIES NAMES libpulsar.a)
if (PULSAR_CPP_LIBRARIES)
    message(STATUS "Found Pulsar C++ library: " ${PULSAR_CPP_LIBRARIES})
else ()
    message(FATAL_ERROR "Failed to find Pulsar C++ library")
endif ()

find_package(PythonLibs REQUIRED)
message(STATUS "PYTHON: " ${PYTHONLIBS_VERSION_STRING})
string(REPLACE "." ";" PYTHONLIBS_VERSION_NO_LIST ${PYTHONLIBS_VERSION_STRING})
list(GET PYTHONLIBS_VERSION_NO_LIST 0 PYTHONLIBS_VERSION_MAJOR)
list(GET PYTHONLIBS_VERSION_NO_LIST 1 PYTHONLIBS_VERSION_MINOR)
set(BOOST_PYTHON_NAME_POSTFIX ${PYTHONLIBS_VERSION_MAJOR}${PYTHONLIBS_VERSION_MINOR})
# For python3 the lib name is boost_python3
set(BOOST_PYTHON_NAME_LIST python37;python38;python39;python310;python3;python3-mt;python-py${BOOST_PYTHON_NAME_POSTFIX};python${BOOST_PYTHON_NAME_POSTFIX}-mt;python${BOOST_PYTHON_NAME_POSTFIX})

set(Boost_NO_BOOST_CMAKE ON)
foreach (BOOST_PYTHON_NAME IN LISTS BOOST_PYTHON_NAME_LIST)
    find_package(Boost QUIET COMPONENTS ${BOOST_PYTHON_NAME})
    if (${Boost_FOUND})
        set(BOOST_PYTHON_NAME_FOUND ${BOOST_PYTHON_NAME})
        break()
    endif()
endforeach()

if (NOT ${Boost_FOUND})
    message(FATAL_ERROR "Could not find Boost Python library")
endif ()
message(STATUS "BOOST_PYTHON_NAME_FOUND: " ${BOOST_PYTHON_NAME_FOUND})
find_package(Boost REQUIRED COMPONENTS ${BOOST_PYTHON_NAME_FOUND})

include_directories(${PULSAR_CPP_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

ADD_LIBRARY(_pulsar SHARED src/pulsar.cc
                           src/producer.cc
                           src/consumer.cc
                           src/config.cc
                           src/enums.cc
                           src/client.cc
                           src/message.cc
                           src/authentication.cc
                           src/reader.cc
                           src/schema.cc
                           src/cryptoKeyReader.cc
                           src/exceptions.cc
                           src/utils.cc
        )

SET(CMAKE_SHARED_LIBRARY_PREFIX )
SET(CMAKE_SHARED_LIBRARY_SUFFIX .so)

if (NOT APPLE AND NOT MSVC)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_PYTHON}")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -Qunused-arguments -undefined dynamic_lookup")
endif()

# Newer boost versions don't use the -mt suffix
if (NOT DEFINED ${Boost_PYTHON37-MT_LIBRARY})
  set(Boost_PYTHON37-MT_LIBRARY ${Boost_PYTHON37_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON38-MT_LIBRARY})
  set(Boost_PYTHON38-MT_LIBRARY ${Boost_PYTHON38_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON39-MT_LIBRARY})
  set(Boost_PYTHON39-MT_LIBRARY ${Boost_PYTHON39_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON310-MT_LIBRARY})
  set(Boost_PYTHON310-MT_LIBRARY ${Boost_PYTHON310_LIBRARY})
endif()

# Try all possible boost-python variable namings
set(PYTHON_WRAPPER_LIBS ${Boost_PYTHON_LIBRARY}
                        ${Boost_PYTHON3_LIBRARY}
                        ${Boost_PYTHON37-MT_LIBRARY}
                        ${Boost_PYTHON38_LIBRARY}
                        ${Boost_PYTHON39_LIBRARY}
                        ${Boost_PYTHON310_LIBRARY}
    )

if (APPLE)
    if (Boost_PYTHON37-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON37-MT_LIBRARY_RELEASE})
    endif ()
    if (Boost_PYTHON38-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON38-MT_LIBRARY_RELEASE})
    endif ()
    if (Boost_PYTHON39-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON39-MT_LIBRARY_RELEASE})
    endif ()
    if (Boost_PYTHON310-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON310-MT_LIBRARY_RELEASE})
    endif ()
endif()

message(STATUS "Using Boost Python libs: ${PYTHON_WRAPPER_LIBS}")

if (NOT PYTHON_WRAPPER_LIBS)
    MESSAGE(FATAL_ERROR "Could not find Boost Python library")
endif ()

if (APPLE)
    set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
    target_link_libraries(_pulsar -Wl,-all_load ${PULSAR_CPP_LIBRARIES} ${PYTHON_WRAPPER_LIBS} ${COMMON_LIBS})
else ()
    if (NOT MSVC)
      set (CMAKE_SHARED_LINKER_FLAGS " -static-libgcc  -static-libstdc++")
    endif()
    target_link_libraries(_pulsar ${PULSAR_CPP_LIBRARIES} ${PYTHON_WRAPPER_LIBS} ${COMMON_LIBS})
endif ()
