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

import shutil
import time
from unittest import SkipTest, TestCase, main
from urllib.error import URLError
from urllib.request import Request, urlopen

import pulsar
from pulsar import AutoClusterFailover, Client, MessageId, ServiceInfo

try:
    from testcontainers.core.container import DockerContainer
except ImportError:
    DockerContainer = None


class AutoClusterFailoverDockerTest(TestCase):

    PRIMARY_URL = "pulsar://localhost:16650"
    PRIMARY_ADMIN_URL = "http://localhost:18080"
    SECONDARY_URL = "pulsar://localhost:26650"
    SECONDARY_ADMIN_URL = "http://localhost:28080"
    RECEIVE_TIMEOUT_MS = 10000
    FAILOVER_WAIT_SECONDS = 30

    @classmethod
    def setUpClass(cls):
        if shutil.which("docker") is None:
            raise SkipTest("docker is required for auto_cluster_failover_test")
        if DockerContainer is None:
            raise SkipTest("testcontainers is required for auto_cluster_failover_test")

        try:
            cls.primary_container = cls._create_container(
                service_port=16650,
                admin_port=18080,
                cluster_name="standalone-0",
            )
            cls.secondary_container = cls._create_container(
                service_port=26650,
                admin_port=28080,
                cluster_name="standalone-1",
            )
            cls.primary_container.start()
            cls.secondary_container.start()
            cls._wait_for_http(cls.PRIMARY_ADMIN_URL + "/metrics")
            cls._wait_for_http(cls.SECONDARY_ADMIN_URL + "/metrics")
            cls._configure_cluster(
                cls.PRIMARY_ADMIN_URL,
                cls.PRIMARY_URL,
                "standalone-0",
            )
            cls._configure_cluster(
                cls.SECONDARY_ADMIN_URL,
                cls.SECONDARY_URL,
                "standalone-1",
            )
        except Exception:
            cls._print_container_logs()
            raise

    @classmethod
    def tearDownClass(cls):
        for container in (
            getattr(cls, "primary_container", None),
            getattr(cls, "secondary_container", None),
        ):
            if container is not None:
                container.stop()

    @classmethod
    def _create_container(cls, service_port, admin_port, cluster_name):
        return (
            DockerContainer("apachepulsar/pulsar:latest")
            .with_env("clusterName", cluster_name)
            .with_env("advertisedAddress", "localhost")
            .with_env("advertisedListeners", f"external:pulsar://localhost:{service_port}")
            .with_env("PULSAR_MEM", "-Xms512m -Xmx512m -XX:MaxDirectMemorySize=256m")
            .with_bind_ports(6650, service_port)
            .with_bind_ports(8080, admin_port)
            .with_command(
                'bash -c "bin/apply-config-from-env.py conf/standalone.conf && '
                'exec bin/pulsar standalone -nss -nfw"'
            )
        )

    @classmethod
    def _print_container_logs(cls):
        for container in (
            getattr(cls, "primary_container", None),
            getattr(cls, "secondary_container", None),
        ):
            if container is None:
                continue
            wrapped = container.get_wrapped_container()
            try:
                print(wrapped.logs().decode("utf-8", errors="replace"))
            except Exception:
                pass

    @classmethod
    def _wait_for_http(cls, url, timeout_seconds=180):
        deadline = time.time() + timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                with urlopen(url, timeout=5) as response:
                    if response.status == 200:
                        return
            except (URLError, OSError) as e:
                last_error = e
            time.sleep(1)
        raise AssertionError(f"Timed out waiting for {url}: {last_error}")

    @staticmethod
    def _http_put(url, data):
        request = Request(url, data.encode("utf-8"))
        request.add_header("Content-Type", "application/json")
        request.get_method = lambda: "PUT"
        try:
            with urlopen(request, timeout=10):
                return
        except URLError as e:
            if "409" in str(e):
                return
            raise

    @classmethod
    def _configure_cluster(cls, admin_url, service_url, cluster_name):
        cls._http_put(
            f"{admin_url}/admin/v2/clusters/{cluster_name}",
            """
            {
              "serviceUrl": "%s/",
              "brokerServiceUrl": "%s/"
            }
            """ % (admin_url, service_url),
        )
        cls._http_put(
            f"{admin_url}/admin/v2/tenants/public",
            """
            {
              "adminRoles": ["anonymous"],
              "allowedClusters": ["%s"]
            }
            """ % cluster_name,
        )
        cls._http_put(
            f"{admin_url}/admin/v2/namespaces/public/default",
            """
            {
              "replication_clusters": ["%s"]
            }
            """ % cluster_name,
        )

    @staticmethod
    def _wait_until(predicate, timeout_seconds, description):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.2)
        raise AssertionError(f"Timed out waiting for {description}")

    @staticmethod
    def _ensure_topic_exists(service_url, topic):
        client = Client(service_url)
        producer = client.create_producer(topic)
        producer.close()
        client.close()

    def test_producer_failover_between_two_standalones(self):
        topic = "test-auto-cluster-failover-%d" % int(time.time() * 1000)
        message_before_failover = b"before-failover"
        message_after_failover = b"after-failover"

        self._ensure_topic_exists(self.PRIMARY_URL, topic)
        self._ensure_topic_exists(self.SECONDARY_URL, topic)

        primary_client = Client(self.PRIMARY_URL)
        primary_reader = primary_client.create_reader(topic, MessageId.earliest)

        secondary_client = Client(self.SECONDARY_URL)
        secondary_reader = secondary_client.create_reader(topic, MessageId.earliest)

        failover_client = Client(
            AutoClusterFailover(
                ServiceInfo(self.PRIMARY_URL),
                [ServiceInfo(self.SECONDARY_URL)],
                check_interval_ms=200,
                failover_threshold=1,
                switch_back_threshold=1,
            ),
            operation_timeout_seconds=10,
        )
        producer = failover_client.create_producer(
            topic,
            send_timeout_millis=3000,
            batching_enabled=False,
        )

        self.assertEqual(failover_client.get_service_info().service_url, self.PRIMARY_URL)

        producer.send(message_before_failover)
        self.assertEqual(primary_reader.read_next(self.RECEIVE_TIMEOUT_MS).data(), message_before_failover)

        primary_reader.close()
        primary_client.close()

        self.primary_container.get_wrapped_container().kill(signal="SIGTERM")

        self._wait_until(
            lambda: failover_client.get_service_info().service_url == self.SECONDARY_URL,
            self.FAILOVER_WAIT_SECONDS,
            "client service info to switch to the secondary broker",
        )

        last_error = None
        deadline = time.time() + self.FAILOVER_WAIT_SECONDS
        while time.time() < deadline:
            try:
                producer.send(message_after_failover)
                break
            except pulsar.PulsarException as e:
                last_error = e
                time.sleep(0.5)
        else:
            raise AssertionError(f"Producer did not recover after failover: {last_error}")

        self.assertEqual(secondary_reader.read_next(self.RECEIVE_TIMEOUT_MS).data(), message_after_failover)
        self.assertEqual(failover_client.get_service_info().service_url, self.SECONDARY_URL)

        producer.close()
        failover_client.close()
        secondary_reader.close()
        secondary_client.close()


if __name__ == "__main__":
    main()
