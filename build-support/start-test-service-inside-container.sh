#!/usr/bin/env bash
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

set -e -x

export PULSAR_EXTRA_OPTS=-Dpulsar.auth.basic.conf=test-conf/.htpasswd

# Show logs immediately for debugging
sed -i.bak 's/immediateFlush: false/immediateFlush: true/' conf/log4j2.yaml

# Generate secret key and token
mkdir -p data/tokens
bin/pulsar tokens create-secret-key --output data/tokens/secret.key

bin/pulsar tokens create \
            --subject token-principal \
            --secret-key file:///pulsar/data/tokens/secret.key \
            > /pulsar/data/tokens/token.txt

# Unset the HTTP proxy to avoid the REST requests being affected
export http_proxy=
TOKEN=$(bin/pulsar tokens create \
            --subject superUser \
            --secret-key file:///pulsar/data/tokens/secret.key)

put() {
    curl -H "Authorization: Bearer $TOKEN" \
        -L http://localhost:8080/admin/v2/$1 \
        -H 'Content-Type: application/json' \
        -X PUT \
        -d $(echo $2 | sed 's/ //g')
}

export PULSAR_STANDALONE_CONF=test-conf/standalone-ssl.conf
export PULSAR_PID_DIR=/tmp
sed -i 's/immediateFlush: false/immediateFlush: true/' conf/log4j2.yaml
bin/pulsar-daemon start standalone \
        --no-functions-worker --no-stream-storage \
        --bookkeeper-dir data/bookkeeper

echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done

echo "-- Pulsar service is ready -- Configure permissions"

export PULSAR_CLIENT_CONF=test-conf/client-ssl.conf

# Create "standalone" cluster if it does not exist
put clusters/standalone '{
  "serviceUrl": "http://localhost:8080/",
  "serviceUrlTls": "https://localhost:8443/",
  "brokerServiceUrl": "pulsar://localhost:6650/",
  "brokerServiceUrlTls": "pulsar+ssl://localhost:6651/"
}'

# Update "public" tenant
put tenants/public '{
  "adminRoles": ["anonymous"],
  "allowedClusters": ["standalone"]
}'

# Update "public/default" with no auth required
put namespaces/public/default '{
  "auth_policies": {
    "namespace_auth": {
      "anonymous": ["produce", "consume"]
    }
  },
  "replication_clusters": ["standalone"]
}'

# Create "public/default-2" with no auth required
put namespaces/public/default-2 '{
  "auth_policies": {
    "namespace_auth": {
      "anonymous": ["produce", "consume"]
    }
  },
  "replication_clusters": ["standalone"]
}'

# Create "private" tenant
put tenants/private '{
  "adminRoles": [],
  "allowedClusters": ["standalone"]
}'

# Create "private/auth" with required authentication
put namespaces/private/auth '{
  "auth_policies": {
    "namespace_auth": {
      "token-principal": ["produce", "consume"]
    }
  },
  "replication_clusters": ["standalone"]
}'

echo "-- Ready to start tests"
