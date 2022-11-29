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

import yaml, sys

if len(sys.argv) < 2:
    print(f'''Usage: {sys.argv[0]} dependency-name [dependency-file]

  The dependency file is "dependencies.yaml" by default.''')
    sys.exit(1)

if len(sys.argv) > 2:
    dependency_file = sys.argv[2]
else:
    dependency_file = 'dependencies.yaml'
deps = yaml.safe_load(open(dependency_file))
print(deps[sys.argv[1]])
