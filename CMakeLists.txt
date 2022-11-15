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

cmake_minimum_required(VERSION 3.18)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

project (pulsar-client-python)
set(CMAKE_PREFIX_PATH ${PROJECT_SOURCE_DIR}/pybind11/include ${CMAKE_PREFIX_PATH})
option(LINK_STATIC "Link against static libraries" OFF)
MESSAGE(STATUS "LINK_STATIC:  " ${LINK_STATIC})

MESSAGE(STATUS "CMAKE_BUILD_TYPE:  " ${CMAKE_BUILD_TYPE})
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
MESSAGE(STATUS "Threads library: " ${CMAKE_THREAD_LIBS_INIT})

if (MSVC)
    add_compile_options(/wd4819)
endif ()

if (LINK_STATIC)
    if (MSVC)
        find_library(PULSAR_LIBRARY NAMES pulsarWithDeps.lib)
    else ()
        find_library(PULSAR_LIBRARY NAMES libpulsar.a)
    endif ()
    add_definitions("-DPULSAR_STATIC")
else()
    find_library(PULSAR_LIBRARY NAMES pulsar libpulsar)
endif()
message(STATUS "PULSAR_LIBRARY: ${PULSAR_LIBRARY}")

find_path(PULSAR_INCLUDE pulsar/Client.h)
message(STATUS "PULSAR_INCLUDE: ${PULSAR_INCLUDE}")

SET(CMAKE_CXX_STANDARD 11)

find_package (Python3 REQUIRED COMPONENTS Development.Module)
MESSAGE(STATUS "PYTHON: " ${Python3_VERSION} " - " ${Python3_INCLUDE_DIRS})

find_path(PYBIND11_INCLUDE_DIRS NAMES "pybind11/pybind11.h")
message(STATUS "PYBIND11_INCLUDE_DIRS: " ${PYBIND11_INCLUDE_DIRS})

########################################################################################################################

INCLUDE_DIRECTORIES(${PULSAR_INCLUDE} ${PYBIND11_INCLUDE_DIRS} ${Python3_INCLUDE_DIRS})

file(GLOB SOURCES src/*.cc)
ADD_LIBRARY(_pulsar SHARED ${SOURCES})
if (MSVC)
    set(CMAKE_SHARED_LIBRARY_SUFFIX .pyd)
else ()
    set(CMAKE_SHARED_LIBRARY_SUFFIX .so)
endif ()

if (NOT APPLE AND NOT MSVC)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_PYTHON}")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -Qunused-arguments -undefined dynamic_lookup")
endif()

set(PYTHON_WRAPPER_LIBS
    ${PULSAR_LIBRARY}
)
if (MSVC)
    set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} Python3::Module)
endif ()

message(STATUS "All libraries: ${PYTHON_WRAPPER_LIBS}")

if (LINK_STATIC AND NOT MSVC)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")

    # We need to include all the static libs individually because we cannot easily create a universal2 libpulsar.a
    # with all the deps included.
    find_package(OpenSSL REQUIRED)
    message("OPENSSL_LIBRARIES: " ${OPENSSL_LIBRARIES})

    find_package(Protobuf REQUIRED)
    message("Protobuf_LIBRARIES: " ${Protobuf_LIBRARIES})

    find_package(curl REQUIRED)
    message("CURL_LIBRARIES: " ${CURL_LIBRARIES})

    find_package(zlib)
    message("ZLIB_LIBRARIES: " ${ZLIB_LIBRARIES})

    find_library(LIB_ZSTD NAMES libzstd.a)
    message(STATUS "ZStd: ${LIB_ZSTD}")
    find_library(LIB_SNAPPY NAMES libsnappy.a)
    message(STATUS "LIB_SNAPPY: ${LIB_SNAPPY}")

    set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS}
                        ${OPENSSL_LIBRARIES}
                        ${Protobuf_LIBRARIES}
                        ${ZLIB_LIBRARIES}
                        ${LIB_ZSTD}
                        ${LIB_SNAPPY}
                        ${CURL_LIBRARIES}
                        )

    if (APPLE)
        set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
        target_link_libraries(_pulsar -Wl,-all_load ${PYTHON_WRAPPER_LIBS})
    else ()
        if (NOT MSVC)
          set (CMAKE_SHARED_LINKER_FLAGS " -static-libgcc  -static-libstdc++")
        endif()
        target_link_libraries(_pulsar ${PYTHON_WRAPPER_LIBS})
    endif ()
elseif (LINK_STATIC) # MSVC
    set_property(TARGET _pulsar PROPERTY
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    target_link_libraries(_pulsar ${PYTHON_WRAPPER_LIBS})
else()
    target_link_libraries(_pulsar ${PYTHON_WRAPPER_LIBS})
endif ()
install(TARGETS _pulsar DESTINATION ${CMAKE_SOURCE_DIR})

find_package(ClangTools)
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        0
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        ${CMAKE_SOURCE_DIR}/src)

# `make check-format` option (for CI test)
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        1
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        ${CMAKE_SOURCE_DIR}/src)
