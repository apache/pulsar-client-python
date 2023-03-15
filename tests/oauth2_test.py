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

from unittest import TestCase, main
from pulsar import AuthenticationOauth2, AuthenticationError, Client
import base64
import os

# This test should run against the standalone that is set up with
# build-support/docker-compose-pulsar-oauth2.yml
class Oauth2Test(TestCase):

    service_url = 'pulsar://localhost:6650'

    def test_invalid_private_key(self):
        with self.assertRaises(ValueError):
            AuthenticationOauth2('{"private_key":"xxx:yyy"}')
        with self.assertRaises(ValueError):
            AuthenticationOauth2('{"private_key":"data:"}')
        with self.assertRaises(ValueError):
            AuthenticationOauth2('{"private_key":"data:application/x-pem"}')
        with self.assertRaises(ValueError):
            AuthenticationOauth2('{"private_key":"data:application/json;xxx"}')

    def test_key_file(self):
        path = (os.path.dirname(os.path.abspath(__file__))
                + '/test-conf/cpp_credentials_file.json')
        auth = AuthenticationOauth2(f'''{{
            "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
            "private_key": "{path}",
            "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        }}''')
        client = Client(self.service_url, authentication=auth)
        producer = client.create_producer('oauth2-test-base64')
        producer.close()
        client.close()

    def test_base64(self):
        credentials = '''{
            "client_id":"Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
            "client_secret":"rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb"
        }'''
        base64_credentials = base64.b64encode(credentials.encode()).decode()
        auth = AuthenticationOauth2(f'''{{
            "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
            "private_key": "data:application/json;base64,{base64_credentials}",
            "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        }}''')
        client = Client(self.service_url, authentication=auth)
        producer = client.create_producer('oauth2-test-base64')
        producer.close()
        client.close()

    def test_wrong_secret(self):
        credentials = '''{
            "client_id": "my-id",
            "client_secret":"my-secret"
        }'''
        base64_credentials = base64.b64encode(credentials.encode()).decode()
        auth = AuthenticationOauth2(f'''{{
            "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
            "private_key": "data:application/json;base64,{base64_credentials}",
            "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        }}''')
        client = Client(self.service_url, authentication=auth)
        with self.assertRaises(AuthenticationError):
            client.create_producer('oauth2-test-base64')
        client.close()

if __name__ == '__main__':
    main()
