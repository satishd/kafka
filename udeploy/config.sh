#!/usr/bin/env bash
# Copyright (c) 2019 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
set -x

mkdir -p /var/log/kafka

# DSC config setup script to be called within the container.
DSC_SETUP_SCRIPT='/shared/dsc/configs/KAFKA/setup-config.sh'
# server.properties file from that worker fetches from dsc.
# The shared directory is specific to a node in a cluster, and hence there will be only one server.properties for the node.
DSC_SERVER_PROPERTIES_FILE_PATH='/shared/dsc/configs/KAFKA/kafka'
# kafka container jvm settings override file
KAFKA_JVM_FILE='kafka_container_jvm_env_vars.sh'

dsc_server_properties_count=$(find ${DSC_SERVER_PROPERTIES_FILE_PATH} -name 'server.properties' | wc -l)

if [ "${dsc_server_properties_count}" -eq "1" ]; then
    # If a single server.properties exists from dsc, go ahead and start Kafka.
    echo "This is cluster which uses configs from DSC"
    rm -rf /tmp/dsc && mkdir -p /tmp/dsc
    bash ${DSC_SETUP_SCRIPT} /tmp/dsc
    cp /tmp/dsc/KAFKA/kafka/*/server.properties /etc/kafka/server.properties
    cp /tmp/dsc/KAFKA/kafka/*/broker-security.properties /etc/kafka/broker-security.properties || true
    cp /tmp/dsc/KAFKA/kafka/*/kafka_server_jaas.conf /etc/kafka/kafka_server_jaas.conf || true
    cp /tmp/dsc/KAFKA/kafka/*/log4j.xml /etc/kafka/log4j.xml || true
    cp ${DSC_SERVER_PROPERTIES_FILE_PATH}/*/${KAFKA_JVM_FILE} /tmp/dsc/
elif [ "${dsc_server_properties}" -gt "1" ]; then
    # If multiple server.properties exists from dsc, bail out.
    echo "Multiple server.properties exists. start.sh script expects only one to exist. Bailing out..."
    exit 1
else
    # If no server.properties exists from dsc, bail out.
    echo "No server.properties exists from dsc via host agent or host gateway. Bailing out."
    exit 1
fi
