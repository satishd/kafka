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

export APP_HOME=${APP_HOME:-/home/udocker/odin-kafka/}
export LOG_HOME=${LOG_HOME:-/var/log/kafka}
export JMX_EXPORTER_VERSION=${JMX_EXPORTER_VERSION:-0.11.0}
export JMX_PORT=${JMX_PORT:-29010}
export JMX_EXPORTER_PORT=${JMX_EXPORTER_PORT:-7071}
export KAFKA_CONF_TARGET_PATH=${APP_HOME}/udeploy
export KAFKA_SERVER_PROPERTIES_LOCATION=${KAFKA_SERVER_PROPERTIES_LOCATION:-${KAFKA_CONF_TARGET_PATH}}
export JVM_HEAP_MEM_MIN=${JVM_HEAP_MEM_MIN:-30G} # 30GB default
export JVM_HEAP_MEM_MAX=${JVM_HEAP_MEM_MAX:-30G} # 30GB default
export JVM_NEW_SIZE_MEM_MIN=${JVM_NEW_SIZE_MEM_MIN:-22G} # 22GB default
export JVM_NEW_SIZE_MEM_MAX=${JVM_NEW_SIZE_MEM_MAX:-22G} # 22GB default
export RSYNC_HEALTHY_WAIT_SEC=${RSYNC_HEALTHY_WAIT_SEC:-300} # 5min default
export SERVER_STARTUP_WAIT_SEC=${SERVER_STARTUP_WAIT_SEC:-0} # 0s by default
HADOOP_CONF_DIR=/opt/hdfs/conf

export DEFAULT_KAFKA_HEAP_OPTS="-Xms${JVM_HEAP_MEM_MIN} \
    -Xmx${JVM_HEAP_MEM_MAX} \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 \
    -XX:NewSize=${JVM_NEW_SIZE_MEM_MIN} -XX:MaxNewSize=${JVM_NEW_SIZE_MEM_MAX} \
    -XX:InitiatingHeapOccupancyPercent=3 \
    -XX:G1MixedGCCountTarget=1 \
    -XX:G1HeapWastePercent=1 \
    -verbose:gc"

export HEAP_OPTS_JDK8=" -XX:+PrintGCDetails \
    -XX:+PrintGCTimeStamps \
    -XX:+PrintGCDateStamps \
    -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M \
    -Xloggc:${LOG_HOME}/gc-kafka.log"

export HEAP_OPTS_JDK11=" -Xlog:gc*:${LOG_HOME}/gc-kafka.log:time,uptime,level,tags:filecount=10,filesize=100M"

export KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS:-${DEFAULT_KAFKA_HEAP_OPTS}}


export DEFAULT_KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:${APP_HOME}/config/log4j.xml"
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/etc/kafka/log4j.xml"

export DEFAULT_KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
                       -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
                       -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT} \
                       -Dcom.sun.management.jmxremote.local.only=false \
                       -Dcom.sun.management.jmxremote.authenticate=false \
                       -Dcom.sun.management.jmxremote.ssl=false"
export KAFKA_JMX_OPTS=${KAFKA_JMX_OPTS:-${DEFAULT_KAFKA_JMX_OPTS}}


#comment out below so kafka container will not start jmx exporter
#export DEFAULT_EXTRA_ARGS="-javaagent:${APP_HOME}/libs/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar=${JMX_EXPORTER_PORT}:${APP_HOME}/udeploy/config/jmx_limited.yaml \
export DEFAULT_EXTRA_ARGS=""
export EXTRA_ARGS=${EXTRA_ARGS:-${DEFAULT_EXTRA_ARGS}}

# RSYNC_PORT default to 29000 if it's not set in the environment variable by odin-kafka-worker
RSYNC_PORT=${RSYNC_PORT:-29000}

OFFLINE_REBUILD_ROOT_PATH=/usr/lib/python2.7/dist-packages/partition_moving_tools
SHARED_RSYNC_PATH=/shared/rsync
OFFLINE_REBUILD_ROOT_PATH_OVERRIDE="${SHARED_RSYNC_PATH}/partition_moving_tools"
OFFLINE_REBUILD_COMMAND_SUFFIX=/scripts/rebuild_broker_odin_kafka_container.sh
OFFLINE_REBUILD_COMMAND_CLEANUP_SUFFIX=/scripts/rebuild_broker_odin_kafka_container_cleanup.sh
