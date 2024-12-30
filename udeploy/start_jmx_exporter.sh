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

source udeploy/vars.sh
bash udeploy/config.sh

# first make sure these 2 env vars is cleaned. 
unset JVM_HEAP_MEM_MIN
unset JVM_HEAP_MEM_MAX

JMXEXPORTER_CONFIG_FILE='jmx_exporter_config.yaml'
DSC_JMXEXPORTER_CONFIG_FILE_PATH='/shared/dsc/configs/KAFKA/kafka'
dsc_jmxexporter_config_file_count=$(find ${DSC_JMXEXPORTER_CONFIG_FILE_PATH} -name ${JMXEXPORTER_CONFIG_FILE} | wc -l)
if [ "${dsc_jmxexporter_config_file_count}" -eq "1" ]; then
    # if a single jmx_exporter_config.yaml exists from dsc, try to get the override, if not, it's ok to use default 
    echo "This is cluster which uses configs from DSC ${JMXEXPORTER_CONFIG_FILE}"
    rm -rf /tmp/dsc && mkdir -p /tmp/dsc
    cp ${DSC_JMXEXPORTER_CONFIG_FILE_PATH}/*/${JMXEXPORTER_CONFIG_FILE} /tmp/dsc/
    if [ -f /tmp/dsc/${JMXEXPORTER_CONFIG_FILE} ]; then
        cp /tmp/dsc/${JMXEXPORTER_CONFIG_FILE} ${APP_HOME}/udeploy/config/jmx_limited.yaml
    fi
else 
    # Print warning. no need to fail, can just use default below.
    # echo "Warning: there are multiple ${JMXEXPORTER_CONFIG_FILE} detected" 
    # if no dsc config, just fail fast. because we might get rid of the default jmx config in the kafka repo in the future
    echo "No DSC found or more than 1 DSC found for jmx_exporter_config.yaml"
    exit 1
fi

JMXEXPORTER_JVM_FILE='jmxexporter_container_jvm_env_vars.sh'
DSC_JMXEXPORTER_JVM_FILE_PATH='/shared/dsc/configs/KAFKA/kafka'
dsc_jmxexporter_jvm_file_count=$(find ${DSC_JMXEXPORTER_JVM_FILE_PATH} -name ${JMXEXPORTER_JVM_FILE} | wc -l)
if [ "${dsc_jmxexporter_jvm_file_count}" -eq "1" ]; then
    # If a single jmxexporter_container_jvm_env_vars.sh exists from dsc, try to get the jvm setting override, if not, it's ok to use default 
    echo "This is cluster which uses configs from DSC ${JMXEXPORTER_JVM_FILE}"
    rm -rf /tmp/dsc && mkdir -p /tmp/dsc
    cp ${DSC_JMXEXPORTER_JVM_FILE_PATH}/*/${JMXEXPORTER_JVM_FILE} /tmp/dsc/
    if [ -f /tmp/dsc/${JMXEXPORTER_JVM_FILE} ]; then
        source /tmp/dsc/${JMXEXPORTER_JVM_FILE}
    fi
else 
    # Print warning. no need to fail, can just use default below.
    echo "Warning: there are multiple ${JMXEXPORTER_JVM_FILE} detected" 
fi

# default JVM for jmxexporter
export JVM_HEAP_MEM_MIN=${JVM_HEAP_MEM_MIN:-2G} # 2GB default
export JVM_HEAP_MEM_MAX=${JVM_HEAP_MEM_MAX:-2G} # 2GB default

# set the JMX_PORT passed by the odin-kafka-worker
JMX_PORT=${JMX_PORT:-29010}

echo "replace the JMX_PORT the environment variable from odin-kafka-worker container"
sed -i -E "s#JMX_PORT#$JMX_PORT#g" ${APP_HOME}/udeploy/config/jmx_limited.yaml

exec /usr/lib/jvm/java-11-openjdk/bin/java -Xms${JVM_HEAP_MEM_MIN} -Xmx${JVM_HEAP_MEM_MAX} -XX:+UseG1GC -XX:MaxGCPauseMillis=2 -jar /usr/share/jmx_exporter/jmx_prometheus_httpserver-0.12.0-jar-with-dependencies.jar ${JMX_EXPORTER_PORT} ${APP_HOME}/udeploy/config/jmx_limited.yaml
