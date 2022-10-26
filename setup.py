#!/usr/bin/env python3
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

from setuptools import setup
from distutils.core import Extension
from os import environ, path
import platform

from distutils.command import build_ext


def get_version():
    # Get the pulsar version from version.txt
    root = path.dirname(path.realpath(__file__))
    version_file = path.join(root, 'version.txt')
    with open(version_file) as f:
        return f.read().strip()


def get_name():
    postfix = environ.get('NAME_POSTFIX', '')
    base = 'pulsar-client'
    return base + postfix


VERSION = get_version()
NAME = get_name()

print('NAME: %s' % NAME)
print('VERSION: %s' % VERSION)


# This is a workaround to have setuptools to include
# the already compiled _pulsar.so library
class my_build_ext(build_ext.build_ext):
    def build_extension(self, ext):
        import shutil
        import os.path

        try:
            os.makedirs(os.path.dirname(self.get_ext_fullpath(ext.name)))
        except OSError as e:
            if e.errno != 17:  # already exists
                raise
        if 'Windows' in platform.platform():
            shutil.copyfile('_pulsar.pyd', self.get_ext_fullpath(ext.name))
        else:
            try:
                shutil.copyfile('_pulsar.so', self.get_ext_fullpath(ext.name))
            except FileNotFoundError:
                shutil.copyfile('lib_pulsar.so', self.get_ext_fullpath(ext.name))


# Core Client dependencies
dependencies = [
    'certifi',
]

extras_require = {}

# functions dependencies
extras_require["functions"] = sorted(
    {
      "protobuf>=3.6.1,<=3.20.*",
      "grpcio<1.28,>=1.8.2",
      "apache-bookkeeper-client>=4.9.2",
      "prometheus_client",
      "ratelimit"
    }
)

# avro dependencies
extras_require["avro"] = sorted(
    {
      "fastavro==0.24.0"
    }
)

# all dependencies
extras_require["all"] = sorted(set(sum(extras_require.values(), [])))

setup(
    name=NAME,
    version=VERSION,
    packages=['pulsar', 'pulsar.schema', 'pulsar.functions'],
    cmdclass={'build_ext': my_build_ext},
    ext_modules=[Extension('_pulsar', [])],

    author="Pulsar Devs",
    author_email="dev@pulsar.apache.org",
    description="Apache Pulsar Python client library",
    license="Apache License v2.0",
    url="https://pulsar.apache.org/",
    install_requires=dependencies,
    extras_require=extras_require,
)
