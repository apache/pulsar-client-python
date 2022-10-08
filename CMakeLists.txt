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
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

option(LINK_STATIC "Link against static libraries" OFF)
MESSAGE(STATUS "LINK_STATIC:  " ${LINK_STATIC})

MESSAGE(STATUS "CMAKE_BUILD_TYPE:  " ${CMAKE_BUILD_TYPE})
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
MESSAGE(STATUS "Threads library: " ${CMAKE_THREAD_LIBS_INIT})

if (LINK_STATIC)
    find_library(PULSAR_LIBRARY NAMES libpulsar.a)
else()
    find_library(PULSAR_LIBRARY NAMES libpulsar.so)
endif()
message(STATUS "PULSAR_LIBRARY: ${PULSAR_LIBRARY}")

find_path(PULSAR_INCLUDE pulsar/Client.h)
message(STATUS "PULSAR_INCLUDE: ${PULSAR_INCLUDE}")

SET(Boost_NO_BOOST_CMAKE ON)
SET(Boost_USE_STATIC_LIBS   ${LINK_STATIC})

SET(CMAKE_CXX_STANDARD 11)

find_package(Boost)

find_package (Python3 COMPONENTS Development)
MESSAGE(STATUS "PYTHON: " ${Python3_VERSION} " - " ${Python3_INCLUDE_DIRS})

string(REPLACE "." ";" PYTHONLIBS_VERSION_NO_LIST ${Python3_VERSION})

set(BOOST_PYTHON_NAME_POSTFIX ${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR})
# For python3 the lib name is boost_python3
set(BOOST_PYTHON_NAME_LIST python${BOOST_PYTHON_NAME_POSTFIX};python37;python38;python39;python310;python3;python3-mt;python-py${BOOST_PYTHON_NAME_POSTFIX};python${BOOST_PYTHON_NAME_POSTFIX}-mt)

foreach (BOOST_PYTHON_NAME IN LISTS BOOST_PYTHON_NAME_LIST)
    find_package(Boost QUIET COMPONENTS ${BOOST_PYTHON_NAME})
    if (${Boost_FOUND})
        set(BOOST_PYTHON_NAME_FOUND ${BOOST_PYTHON_NAME})
        break()
    endif()
endforeach()

if (NOT ${Boost_FOUND})
    MESSAGE(FATAL_ERROR "Could not find Boost Python library")
endif ()

MESSAGE(STATUS "BOOST_PYTHON_NAME_FOUND: " ${BOOST_PYTHON_NAME_FOUND})

########################################################################################################################

INCLUDE_DIRECTORIES(${PULSAR_INCLUDE} "${Boost_INCLUDE_DIRS}" "${Python3_INCLUDE_DIRS}")

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
set(PYTHON_WRAPPER_LIBS ${PULSAR_LIBRARY}
                        ${Boost_PYTHON_LIBRARY}
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

if (NOT PYTHON_WRAPPER_LIBS)
    MESSAGE(FATAL_ERROR "Could not find Boost Python library")
endif ()

message(STATUS "All libraries: ${PYTHON_WRAPPER_LIBS}")

if (LINK_STATIC)
    if (APPLE)
        set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
        target_link_libraries(_pulsar -Wl,-all_load ${PYTHON_WRAPPER_LIBS})
    else ()
        if (NOT MSVC)
          set (CMAKE_SHARED_LINKER_FLAGS " -static-libgcc  -static-libstdc++")
        endif()
        target_link_libraries(_pulsar ${PYTHON_WRAPPER_LIBS})
    endif ()
else()
    target_link_libraries(_pulsar ${PYTHON_WRAPPER_LIBS})
endif ()

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
