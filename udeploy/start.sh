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

# Exit with an error message.
die() {
    echo $@
    exit 1
}

bash udeploy/config.sh
KAFKA_JVM_FILE='kafka_container_jvm_env_vars.sh'
if [ -f /tmp/dsc/${KAFKA_JVM_FILE} ]
then
    source /tmp/dsc/${KAFKA_JVM_FILE}
fi

source udeploy/vars.sh
source udeploy/kraft_utils.sh

# CLUSTER, UBER_PORT_KAFKA are replaced in odin-kafka-worker (src/code.uber.internal/storage/odin/kafka/worker/pkg/worker/dsc.go)
export UBER_SECURE_PORT_KAFKA=9443

find /etc/kafka -maxdepth 1 -type f -print0 | while read -d $'\0' file; do
  sed -i -e "s/##UBER_SECURE_PORT_KAFKA##/${UBER_SECURE_PORT_KAFKA}/g" $file || true
  sed -i -e "s/##UBER_REGION##/${UBER_REGION}/g" $file || true
done

if cat /etc/kafka/server.properties | grep -E "^remote.log.storage.system.enable=true|^zookeeper.set.acl=true" ; then
  KRB5_CONF_SETUP=/opt/uber-data-krb5-conf/scripts/setup.sh
  KRB5_CONFIG=/etc/kafka/krb5.conf
  if [ ! -f "${KRB5_CONF_SETUP}" ]; then
    echo "KRB5 conf setup package is not available. Exiting."
    exit 1
  fi

  # Connect to Kerberos `beta` environment from kafka-staging1 clusters. See DKAFC-4585 ticket.
  # https://sourcegraph.uberinternal.com/code.uber.internal/data/krb5-conf/-/blob/scripts/setup.sh
  if [[ ${NODE_NAME} =~ staging1 ]]; then
    echo "Setting up KDC env to beta for staging1 nodes."
    KDC_ENV='--kdc-env beta'
  fi

  ${KRB5_CONF_SETUP} --region ${UBER_REGION} --env ${UBER_RUNTIME_ENVIRONMENT} ${KDC_ENV} --dest ${KRB5_CONFIG}
  # Note that Hadoop won't read the KRB5_CONFIG variable even if exported, the krb5 conf needs to be supplied as
  # system property.
  export EXTRA_ARGS="${EXTRA_ARGS} -Djava.security.krb5.conf=${KRB5_CONFIG}"
fi

if cat /etc/kafka/server.properties | grep -e "^remote.log.storage.system.enable=true"; then
  /opt/uber-data-hdfs-conf/scripts/setup.sh --region ${UBER_REGION} --conf ${HADOOP_CONF_DIR} --skystream true --runtime staging
  # override the core-site.xml file after generation
  cp /tmp/dsc/KAFKA/kafka/*/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml || true
fi

if cat /etc/kafka/server.properties | grep -e "^zookeeper.set.acl=true"; then
  # JAAS config
  JAAS_CONFIG=/etc/kafka/kafka_server_jaas.conf
  if [ ! -f "${JAAS_CONFIG}" ]; then
    echo "JAAS conf file not available"
    exit 1
  fi
  export EXTRA_ARGS="${EXTRA_ARGS} -Djava.security.auth.login.config=${JAAS_CONFIG}"
fi

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS} ${HEAP_OPTS_JDK17}"
export EXTRA_ARGS="${EXTRA_ARGS} --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED"
# Check if cluster is currently setup with secure settings. If true, add secure to Kafka lib folder
if cat /etc/kafka/server.properties | grep -e "^listeners=" | grep -q "SSL://"; then
  # EXTRA_ARGS is picked up by kafka-server-start.sh
  export EXTRA_ARGS="${EXTRA_ARGS} -Dupki.properties=/etc/kafka/broker-security.properties \
                                   -Dcom.uber.engsec.auth.bouncycastle-enabled=false"
fi

if [ ! -f "$(echo ${KAFKA_LOG4J_OPTS} | cut -d: -f2)" ]; then
  # Fallback to the default, if the configured file is not available.
  export KAFKA_LOG4J_OPTS="${DEFAULT_KAFKA_LOG4J_OPTS}"
fi

# This method has to always be the last call that this script makes and please ensure that this call happens in the foreground.
# This ensures that PID 1 control is handed over to the kafka process.
# More details - https://docs.google.com/document/d/195VSlMBTL2unH091mcR_wAKj_NnPp-5Y7pTRXChhslM/edit?tab=t.0
function start_kafka() {
    # Workaround for https://issues.apache.org/jira/browse/KAFKA-7235
    echo "Sleeping for ${SERVER_STARTUP_WAIT_SEC} seconds before starting the server..."
    sleep "${SERVER_STARTUP_WAIT_SEC}"
    exec ${APP_HOME}/bin/kafka-server-start.sh /etc/kafka/server.properties
}

KAFKA_CONTAINER_OFFLINE_REBUILD_FILE=/shared/KAFKA_CONTAINER_OFFLINE_REBUILD
KAFKA_CONTAINER_DATA_DIR=/shared/data1/data

OFFLINE_REBUILD_COMMAND="${OFFLINE_REBUILD_ROOT_PATH}${OFFLINE_REBUILD_COMMAND_SUFFIX}"
OFFLINE_REBUILD_COMMAND_CLEANUP="${OFFLINE_REBUILD_ROOT_PATH}${OFFLINE_REBUILD_COMMAND_CLEANUP_SUFFIX}"

function override_rebuild_path_with_rsync() {
  max_retry=${RSYNC_HEALTHY_WAIT_SEC}
  for n in $(seq 1 "${max_retry}"); do
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/${RSYNC_PORT}"; then
      if [ -d "${OFFLINE_REBUILD_ROOT_PATH_OVERRIDE}" ]; then
        OFFLINE_REBUILD_COMMAND="${OFFLINE_REBUILD_ROOT_PATH_OVERRIDE}${OFFLINE_REBUILD_COMMAND_SUFFIX}"
        OFFLINE_REBUILD_COMMAND_CLEANUP="${OFFLINE_REBUILD_ROOT_PATH_OVERRIDE}${OFFLINE_REBUILD_COMMAND_CLEANUP_SUFFIX}"
      else
        echo "Couldn't find ${OFFLINE_REBUILD_ROOT_PATH_OVERRIDE}, using the fallback rebuild path"
      fi
      echo "Final rsync commands: ${OFFLINE_REBUILD_COMMAND} , ${OFFLINE_REBUILD_COMMAND_CLEANUP}"
      break
    fi
    echo "rsync container not running, trying again... (${n}/${max_retry})"
    sleep 1
  done
}

#no need to use JMX_PORT anymore
export JMX_PORT=""

# Check if this is a controller-only node
if is_controller_only; then
    # Check for controller initialization marker file
    if [ -f "/shared/INITIALIZE_QUORUM" ]; then
        # Marker present: format as standalone (--ignore-formatted handles idempotency)
        echo "=== Initializing Standalone Controller ==="
        format_standalone_controller
        if [ $? -eq 0 ]; then
            start_kafka
        else
            die "Failed to initialize standalone controller"
        fi
    else
        # Marker absent: join existing quorum (--ignore-formatted handles idempotency)
        echo "=== Joining Existing Quorum ==="

        format_controller_for_quorum
        if [ $? -ne 0 ]; then
            die "Failed to format controller for quorum"
        fi

        # Start background task to add controller to quorum after startup
        # Note: Only needed if freshly formatted, but safe to run always
        # The background process will continue independently after exec replaces this process
        join_controller_to_quorum_background &
        BACKGROUND_PID=$!
        echo "Started background quorum join task (PID: ${BACKGROUND_PID})"

        # Setup trap to cleanup background process if this script fails before exec
        trap "echo 'Cleaning up background process ${BACKGROUND_PID}'; kill ${BACKGROUND_PID} 2>/dev/null || true" EXIT

        # Start Kafka server in foreground (takes over PID 1)
        # Note: exec replaces this process, so trap won't fire on successful startup
        echo "Starting controller server..."
        start_kafka
    fi
elif [ -f "${KAFKA_CONTAINER_OFFLINE_REBUILD_FILE}" ]; then
    # Existing offline rebuild logic for broker or combined mode
    echo "Detected broker mode - checking for offline rebuild"

    # to prevent endless retry when something wrong and the offline rebuild keeps erroring out. for now retry=0, this logic can be enhanced to allow some retries.
    if [ -f "${KAFKA_CONTAINER_OFFLINE_REBUILD_FILE}.previous_run" ]
    then
        while true
        do
           echo "something was wrong, please check the /var/log/kafka/rebuild.log, do an infite loop here to prevent containter keeps getting restarted"
           echo "to re-enable the offline rebuild(if the issues are identified and fixd), remove the KAFKA_CONTAINER_OFFLINE_REBUILD.previous_run file. e.g.: "
           echo "sudo docker exec $(sudo docker ps |grep kafka |grep worker$ |awk '{print $NF}') rm /volumes/shared/KAFKA_CONTAINER_OFFLINE_REBUILD.previous_run"
           sleep 300
        done
    fi
    touch "${KAFKA_CONTAINER_OFFLINE_REBUILD_FILE}.previous_run"

    # It needs to do offline rebuild first.
    # if the kafka container crashed or retarted (e.g. version upgrade), the rebuild can start from beginning. It's an idempotent operation.
    echo "Checking if rsync has a shared offline rebuild path which can be used"
    override_rebuild_path_with_rsync
    echo "kick off rebuild script"
    ${OFFLINE_REBUILD_COMMAND} >>/var/log/kafka/rebuild.log 2>&1
    echo "after rsync complete, if it's successful, starting kafka"
    if [ -f "${KAFKA_CONTAINER_OFFLINE_REBUILD_FILE}.done" ]
    then
        echo "kick off the clean up after rsync and delta catch-up"
        echo "start kafka process"
        # Running the cleanup in background and handing over the control to start_kafka
        { unset JMX_PORT; unset KAFKA_JMX_OPTS; sleep 10; ${OFFLINE_REBUILD_COMMAND_CLEANUP} >>/var/log/kafka/rebuild.log 2>&1; } &
        start_kafka
    fi
else
    echo "start kafka process as normal"
    start_kafka
fi
