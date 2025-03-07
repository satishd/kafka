# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import json

from ducktape.services.service import Service
from ducktape.utils.util import wait_until


class CruiseControl(Service):
    BINARY_LOC = "/opt/cruise-control/cruise-control.jar"
    PERSISTENT_ROOT = "/mnt/cruise-control"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "kafkacruisecontrol.log")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "cruisecontrol.properties")
    CAPACITY_FILE = os.path.join(PERSISTENT_ROOT, "capacityCores.json")
    LOGBACK_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "logback.xml")
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "cruise_control.stdout-stderr.log")

    logs = {
        "cruise_control_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
        "cruise_control_log": {
            "path": LOG_FILE,
            "collect_default": True
        }
    }

    def __init__(self, test_context, bootstrap_servers, zk_connect, disk_capacity_threshold, self_healing_enabled, port=9090):
        super(CruiseControl, self).__init__(test_context, num_nodes=1)
        self.bootstrap_servers = bootstrap_servers
        self.zk_connect = zk_connect
        self.port = port
        self.disk_capacity_threshold = disk_capacity_threshold
        self.self_healing_enabled = self_healing_enabled

    def start_cmd(self, node):
        cmd = "%s 1>> %s 2>> %s &" % \
              (("java -Dlogback.configurationFile=%s -cp %s com.linkedin.kafka.cruisecontrol.KafkaCruiseControlMain "
                "%s %s %s") %
               (self.LOGBACK_CONFIG_FILE, self.BINARY_LOC, self.CONFIG_FILE, self.port, node.account.hostname),
               self.STDOUT_STDERR_CAPTURE,
               self.STDOUT_STDERR_CAPTURE)
        return cmd

    def start_node(self, node, timeout_sec=60):
        self.context.logger.info("Starting Cruise Control on %s" % node.account.hostname)
        # Create necessary directories
        node.account.mkdirs(self.PERSISTENT_ROOT)

        # Create logback.xml file
        logback_file = self.render("logback_template.xml", log_dir=self.PERSISTENT_ROOT)
        node.account.create_file(self.LOGBACK_CONFIG_FILE, logback_file)

        # Create capacityCores.json file
        capacity_file = self.render("capacityCores.json")
        node.account.create_file(self.CAPACITY_FILE, capacity_file)

        # Create cruisecontrol.properties file
        cruise_control_properties = self.render("cruisecontrol_template.properties", zookeeper_connect=self.zk_connect,
                                                bootstrap_servers=self.bootstrap_servers,
                                                disk_capacity_threshold=self.disk_capacity_threshold,
                                                self_healing_enabled=self.self_healing_enabled)
        node.account.create_file(self.CONFIG_FILE, cruise_control_properties)

        start_cmd = self.start_cmd(node)
        with node.account.monitor_log(self.LOG_FILE) as monitor:
            node.account.ssh(start_cmd, allow_fail=False)
            monitor.wait_until("Kafka Cruise Control started", timeout_sec=timeout_sec, backoff_sec=2,
                               err_msg="Cruise Control failed to start in %d seconds" % timeout_sec)

    def state(self, node=None, json=True, verbose=True):
        self.context.logger.info("Fetch cruise control state")
        if node is None:
            node = self.nodes[0]

        cmd = ("curl -s 'http://%s:%d/kafkacruisecontrol/state?json=%s&verbose=%s'" %
               (node.account.hostname, self.port, json, verbose))
        return self._execute_cmd_and_capture_result(cmd, node)

    def wait_till_proposals_generated(self, node=None, timeout_sec=600):
        self.context.logger.info("Wait until cruise control proposals are generated")
        if node is None:
            node = self.nodes[0]

        wait_until(lambda: json.loads(self.state(node))['AnalyzerState']['isProposalReady'], timeout_sec=timeout_sec,
                   backoff_sec=30, err_msg="Cruise control proposals not generated in %d seconds" % timeout_sec)

    def proposals(self, node=None, json=True, verbose=True, goals=None):
        self.context.logger.info("Fetch cruise control proposals")
        if node is None:
            node = self.nodes[0]

        if goals is None:
            cmd = ("curl -s 'http://%s:%d/kafkacruisecontrol/proposals?json=%s&verbose=%s'" %
                   (node.account.hostname, self.port, json, verbose))
        else:
            cmd = ("curl -s 'http://%s:%d/kafkacruisecontrol/proposals?json=%s&verbose=%s&goals=%s'" %
                   (node.account.hostname, self.port, json, verbose, goals))
        return self._execute_cmd_and_capture_result(cmd, node)

    def wait_till_goal_violation_alerts(self, monitor, pattern, timeout_sec=600, backoff_sec=2):
        self.context.logger.info("Fetch cruise control broker failure alerts")
        monitor.wait_until("GOAL_VIOLATION detected.*%s" % pattern, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                           err_msg="Cruise control goal violation alerts not found in %d seconds" % timeout_sec)

    def wait_till_broker_failure_alerts(self, monitor, timeout_sec=600):
        self.context.logger.info("Fetch cruise control broker failure alerts")
        monitor.wait_until("BROKER_FAILURE detected", timeout_sec=timeout_sec, backoff_sec=2,
                           err_msg="Cruise control broker failure alerts not found in %d seconds" % timeout_sec)

    def wait_till_disk_failure_alerts(self, monitor, timeout_sec=600):
        self.context.logger.info("Fetch cruise control disk failure alerts")
        monitor.wait_until("DISK_FAILURE detected", timeout_sec=timeout_sec, backoff_sec=2,
                           err_msg="Cruise control disk failure alerts not found in %d seconds" % timeout_sec)

    def rebalance(self, node=None, goals=None, dry_run=False, json=True, verbose=True):
        self.context.logger.info("Rebalance the cluster")
        if node is None:
            node = self.nodes[0]

        if goals is None:
            cmd = ("curl -s -X POST 'http://%s:%d/kafkacruisecontrol/rebalance?dryRun=%s&json=%s&verbose=%s'" %
                   (node.account.hostname, self.port, dry_run, json, verbose))
        else:
            cmd = ("curl -s -X POST 'http://%s:%d/kafkacruisecontrol/rebalance?dryRun=%s&goals=%s&json=%s&verbose=%s'" %
                   (node.account.hostname, self.port, dry_run, goals, json, verbose))
        return self._execute_cmd_and_capture_result(cmd, node)

    def user_task_status(self, user_task_ids, node=None, json=True):
        self.context.logger.info("Fetch cruise control request status")
        if node is None:
            node = self.nodes[0]

        cmd = ("curl -s 'http://%s:%d/kafkacruisecontrol/user_tasks?json=%s&user_task_ids=%s'" %
               (node.account.hostname, self.port, json, user_task_ids))
        return self._execute_cmd_and_capture_result(cmd, node)

    def stop_node(self, node):
        node.account.kill_process("KafkaCruiseControlMain", allow_fail=False)

    def clean_node(self, node):
        node.account.kill_process("KafkaCruiseControlMain", allow_fail=False)
        node.account.ssh("rm -rf %s" % self.PERSISTENT_ROOT, allow_fail=False)

    def _execute_cmd_and_capture_result(self, cmd, node):
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        self.logger.debug(output)
        return output
