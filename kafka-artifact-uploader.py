#!/usr/bin/env python

#
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
#

import os
import subprocess
import sys


def main():
    """
    https://engwiki.uberinternal.com/display/CODE/Artifactory+User+Guide#ArtifactoryUserGuide-GenericArtifacts
    https://docs.google.com/document/d/1d7H1FIBXoe7eATK2wHPS0kzfrnAMaQJzLIWbw9GomJk

    $ token=$(usso -ussh artifacts -print)
    $ python3 kafka-artifact-uploader.py $token /tmp/myRepo 2.9.68-2.12-uber
    """
    if len(sys.argv) < 4:
        print("Usage: python3 kafka-artifact-uploader.py <token> <local_repo_path> <version>. \n"
              " (eg) python3 kafka-artifact-uploader.py $token /tmp/myRepo 2.9.68-2.12-uber")
        sys.exit(1)

    token = sys.argv[1]
    local_repo = sys.argv[2]
    version = sys.argv[3]
    # modules = os.listdir(f"{local_repo}/org/apache/kafka")
    modules = ["connect-api", "connect-json", "kafka-clients", "kafka-metadata", "kafka-raft", "kafka-server-common",
               "kafka-storage", "kafka-storage-api", "kafka-streams", "kafka_2.12",
               # the below modules are optional to upload
               "kafka-log4j-appender", "kafka-shell", "kafka-tools", "remote-storage-managers", "kafka-rsm-hdfs"]
    print(modules)
    for module in modules:
        base_path = f"{local_repo}/org/apache/kafka/{module}/{version}"
        artifactory_url = f"https://artifacts.uberinternal.com/artifactory/libs-release-local/org/apache/kafka/{module}/{version}"

        # Upload only the jar and pom files
        for filename in os.listdir(base_path):
            filepath = os.path.join(base_path, filename)
            if filename.endswith(('.pom', '.jar')):
                print(f"Uploading: {filename}")
                auth_header = f'Authorization: Bearer {token}'
                cmd = ['curl', '-H', auth_header, f"{artifactory_url}/{filename}", '-T', filepath]
                # print(cmd)
                subprocess.run(cmd)


if __name__ == "__main__":
    main()