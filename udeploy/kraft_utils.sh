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

# KRaft Controller Utility Functions
# This file contains functions for KRaft controller initialization,
# formatting, quorum management, and readiness checks.

# Detect if this node is running in controller-only mode
is_controller_only() {
    # Extract process.roles from server.properties
    local PROCESS_ROLES=$(grep "^process.roles=" /etc/kafka/server.properties 2>/dev/null | cut -d'=' -f2 | tr -d '[:space:]')

    # Check if it's exactly "controller" (not "broker" or "broker,controller")
    if [ "$PROCESS_ROLES" = "controller" ]; then
        return 0  # true - is controller-only
    else
        return 1  # false - is broker or combined mode
    fi
}

# Get metadata log directory from server.properties
# Returns the metadata.log.dir if set, otherwise falls back to first log.dirs entry
get_metadata_log_dir() {
    local METADATA_LOG_DIR=$(grep "^metadata.log.dir=" /etc/kafka/server.properties | cut -d= -f2)
    if [ -z "${METADATA_LOG_DIR}" ]; then
        METADATA_LOG_DIR=$(grep "^log.dirs=" /etc/kafka/server.properties | cut -d= -f2 | cut -d, -f1)
    fi
    if [ -z "${METADATA_LOG_DIR}" ]; then
        METADATA_LOG_DIR=$(grep "^log.dir=" /etc/kafka/server.properties | cut -d= -f2 | cut -d, -f1)
    fi
    echo "${METADATA_LOG_DIR}"
}

# Get cluster ID from /etc/kafka/metadata.properties file
# CRITICAL: This file MUST exist and contain a valid cluster ID in properties format
# The cluster metadata file is published by odin-kafka-worker during ensureClusterMetadata()
# and copied to /etc/kafka/ by config.sh from the DSC config directory
# Properties format: cluster_id=MkU3OEVBNjY4NjU0NDQx
get_cluster_id() {
    local CLUSTER_METADATA_PROPERTIES="/etc/kafka/metadata.properties"

    # Step 1: Check if file exists
    if [ ! -f "$CLUSTER_METADATA_PROPERTIES" ]; then
        echo "ERROR: Cluster metadata file not found at ${CLUSTER_METADATA_PROPERTIES}" >&2
        echo "ERROR: The cluster metadata file must be published by odin-kafka-worker before container startup" >&2
        echo "ERROR: This file should be written during worker's ensureClusterMetadata() execution" >&2
        echo "ERROR: and copied to /etc/kafka/ by config.sh from DSC configs" >&2
        return 1
    fi

    # Step 2: Check if file is readable
    if [ ! -r "$CLUSTER_METADATA_PROPERTIES" ]; then
        echo "ERROR: Cluster metadata file exists but is not readable: ${CLUSTER_METADATA_PROPERTIES}" >&2
        echo "ERROR: Check file permissions (current: $(ls -l ${CLUSTER_METADATA_PROPERTIES} 2>&1))" >&2
        return 1
    fi

    # Step 3: Parse properties file and extract cluster_id
    local CLUSTER_ID
    CLUSTER_ID=$(grep "^cluster_id=" "$CLUSTER_METADATA_PROPERTIES" 2>&1 | cut -d= -f2 | tr -d '[:space:]')

    # Step 4: Check if parsing succeeded (grep returns non-zero if pattern not found)
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to read cluster_id from ${CLUSTER_METADATA_PROPERTIES}" >&2
        echo "ERROR: grep error: ${CLUSTER_ID}" >&2
        echo "ERROR: Verify that the file contains cluster_id=<value>" >&2
        return 1
    fi

    # Step 5: Check if result is empty
    if [ -z "$CLUSTER_ID" ]; then
        echo "ERROR: Cluster ID extracted from properties file is empty: ${CLUSTER_METADATA_PROPERTIES}" >&2
        echo "ERROR: File content: $(cat ${CLUSTER_METADATA_PROPERTIES} 2>&1)" >&2
        return 1
    fi

    # Step 6: Success - log and return
    echo "INFO: Successfully read cluster ID from ${CLUSTER_METADATA_PROPERTIES}: ${CLUSTER_ID}" >&2
    echo "$CLUSTER_ID"
}

# Extract first bootstrap controller from server.properties
get_bootstrap_controller() {
    local BOOTSTRAP_SERVERS=$(grep "^controller.quorum.bootstrap.servers=" /etc/kafka/server.properties 2>/dev/null | cut -d'=' -f2 | tr -d '[:space:]')

    if [ -z "$BOOTSTRAP_SERVERS" ]; then
        echo "ERROR: controller.quorum.bootstrap.servers not found in server.properties" >&2
        return 1
    fi

    # Get first server from comma-separated list
    local FIRST_SERVER=$(echo "$BOOTSTRAP_SERVERS" | cut -d',' -f1)
    echo "$FIRST_SERVER"
}

# Format controller with specified mode
# Args:
#   $1 - mode: "standalone" or "join-quorum"
# Returns:
#   0 on success, 1 on failure
format_controller() {
    local MODE="$1"

    # Validate mode parameter
    if [[ "$MODE" != "standalone" && "$MODE" != "join-quorum" ]]; then
        echo "ERROR: Invalid mode '$MODE'. Must be 'standalone' or 'join-quorum'"
        return 1
    fi

    # Set mode-specific configuration
    local FORMAT_FLAG
    local LOG_PREFIX
    local SUCCESS_SUFFIX

    if [ "$MODE" = "standalone" ]; then
        LOG_PREFIX="Formatting controller as standalone (single-node quorum)"
        FORMAT_FLAG="--standalone"
        SUCCESS_SUFFIX="standalone controller"
    else
        LOG_PREFIX="Formatting controller to join existing quorum"
        FORMAT_FLAG="--no-initial-controllers"
        SUCCESS_SUFFIX="controller"
    fi

    echo "${LOG_PREFIX}..."

    # Get cluster ID
    local CLUSTER_ID=$(get_cluster_id)
    if [ -z "${CLUSTER_ID}" ]; then
        echo "ERROR: Failed to get cluster ID"
        return 1
    fi

    # Format with appropriate flag
    ${APP_HOME}/bin/kafka-storage.sh format \
        --cluster-id "${CLUSTER_ID}" \
        --config /etc/kafka/server.properties \
        ${FORMAT_FLAG} \
        --ignore-formatted

    if [ $? -eq 0 ]; then
        echo "Successfully formatted ${SUCCESS_SUFFIX} with cluster ID: ${CLUSTER_ID}"
        if [ "$MODE" = "join-quorum" ]; then
            echo "Controller will join quorum after startup"
        fi
        return 0
    else
        echo "ERROR: Failed to format ${SUCCESS_SUFFIX}"
        return 1
    fi
}

# Format controller as standalone (single-node quorum)
format_standalone_controller() {
    format_controller "standalone"
}

# Format controller to join existing quorum (without initial controllers)
format_controller_for_quorum() {
    format_controller "join-quorum"
}

# TODO: https://t3.uberinternal.com/browse/DKAFC-6968 Simplify add controller logic for lag catch up
# Wait for controller to be healthy and replication lag to be 0
wait_for_controller_ready() {
    local MAX_WAIT_SECONDS=${CONTROLLER_READINESS_WAIT_SECONDS}
    local CHECK_INTERVAL=5      # Check every 5 seconds
    local elapsed=0

    if [ $MAX_WAIT_SECONDS -eq 0 ]; then
        echo "Waiting for controller to be ready and replication lag to settle (infinite wait)..."
    else
        echo "Waiting for controller to be ready and replication lag to settle (timeout: ${MAX_WAIT_SECONDS}s)..."
    fi

    # Emit metric indicating start of readiness wait (value 0 = no delay yet)
    send_m3_metric "controller.readiness.delay" "0"

    # Get bootstrap controller
    local BOOTSTRAP_CONTROLLER=$(get_bootstrap_controller)
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to get bootstrap controller"
        return 1
    fi

    # Determine metadata log directory
    local METADATA_LOG_DIR=$(get_metadata_log_dir)

    # Read directory ID from meta.properties
    local META_PROPERTIES_FILE="${METADATA_LOG_DIR}/meta.properties"
    local DIRECTORY_ID=""

    if [ -f "${META_PROPERTIES_FILE}" ]; then
        DIRECTORY_ID=$(grep "^directory.id=" "${META_PROPERTIES_FILE}" | cut -d= -f2 | tr -d '[:space:]')
        if [ -n "${DIRECTORY_ID}" ]; then
            echo "Found directory ID for this controller: ${DIRECTORY_ID}"
        else
            echo "WARNING: meta.properties exists but directory.id is empty or not found"
            echo "WARNING: Will retry reading directory.id on each check iteration"
        fi
    else
        echo "WARNING: meta.properties not found at ${META_PROPERTIES_FILE}"
        echo "WARNING: This is expected immediately after formatting - will retry on each iteration"
    fi

    while [ $MAX_WAIT_SECONDS -eq 0 ] || [ $elapsed -lt $MAX_WAIT_SECONDS ]; do
        # Re-read directory ID if not found in previous attempts
        if [ -z "${DIRECTORY_ID}" ]; then
            if [ -f "${META_PROPERTIES_FILE}" ]; then
                DIRECTORY_ID=$(grep "^directory.id=" "${META_PROPERTIES_FILE}" | cut -d= -f2 | tr -d '[:space:]')
                if [ -n "${DIRECTORY_ID}" ]; then
                    echo "Successfully read directory ID: ${DIRECTORY_ID}"
                fi
            fi
        fi

        # Check replication status using kafka-metadata-quorum.sh
        local REPLICATION_OUTPUT=$(unset JMX_PORT; unset KAFKA_JMX_OPTS; unset KAFKA_HEAP_OPTS; ${APP_HOME}/bin/kafka-metadata-quorum.sh \
            --bootstrap-controller "${BOOTSTRAP_CONTROLLER}" \
            describe --replication 2>/dev/null)

        if [ $? -eq 0 ]; then
            # If we have directory ID, find lag for THIS specific controller
            if [ -n "${DIRECTORY_ID}" ]; then
                # Extract lag for the row where column 2 matches DIRECTORY_ID
                local CONTROLLER_LAG=$(echo "$REPLICATION_OUTPUT" | grep -F "${DIRECTORY_ID}" | awk '{print $4}' | grep -E '^[0-9]+$')

                if [ -n "$CONTROLLER_LAG" ]; then
                    # Found this controller's lag
                    if [ "$CONTROLLER_LAG" -eq 0 ]; then
                        echo "Controller (directory ID: ${DIRECTORY_ID}) is ready - replication lag is 0"
                        return 0
                    else
                        echo "Controller (directory ID: ${DIRECTORY_ID}) replication lag: ${CONTROLLER_LAG}, waiting... (${elapsed}s elapsed)"
                    fi
                else
                    # Directory ID not found in replication output
                    echo "WARNING: Controller with directory ID ${DIRECTORY_ID} not found in replication output, waiting... (${elapsed}s elapsed)"
                fi
            else
                # Directory ID still not available - log warning and continue
                echo "WARNING: Cannot read directory.id from ${META_PROPERTIES_FILE}, waiting... (${elapsed}s elapsed)"
            fi
        else
            echo "Controller not yet responding to metadata quorum queries (${elapsed}s elapsed)"
        fi

        # Log milestones in infinite wait mode to provide visibility
        if [ $MAX_WAIT_SECONDS -eq 0 ]; then
            case $elapsed in
                300)  # 5 minutes
                    echo "INFO: Still waiting for controller replication lag to reach 0 (${elapsed}s elapsed, infinite wait mode)"
                    ;;
                600)  # 10 minutes
                    echo "WARNING: Controller replication lag convergence taking longer than expected (${elapsed}s elapsed)"
                    echo "WARNING: Check cluster health and network connectivity"
                    ;;
                1800)  # 30 minutes
                    # Emit metric indicating prolonged delay (value 1 = delay detected)
                    send_m3_metric "controller.readiness.delay" "1"

                    echo "WARNING: Controller replication lag has not converged after ${elapsed}s"
                    echo "WARNING: This may indicate network issues, cluster overload, or configuration problems"
                    echo "WARNING: Check: 1) Network connectivity 2) Disk I/O 3) CPU/Memory 4) Quorum health"
                    ;;
                3600)  # 60 minutes
                    echo "CRITICAL WARNING: Controller replication lag convergence extremely delayed (${elapsed}s elapsed)"
                    echo "CRITICAL WARNING: Manual investigation strongly recommended"
                    echo "CRITICAL WARNING: Consider checking kafka-metadata-quorum.sh describe --status for quorum health"
                    ;;
                *)
                    # Log every hour after the 60-minute mark (for extreme delays)
                    if [ $elapsed -gt 3600 ] && [ $((elapsed % 3600)) -eq 0 ]; then
                        local hours=$((elapsed / 3600))
                        echo "CRITICAL WARNING: Controller replication lag still not converged after ${hours} hour(s) (${elapsed}s elapsed)"
                        echo "CRITICAL WARNING: This indicates a serious problem - immediate investigation required"
                    fi
                    ;;
            esac
        fi

        sleep $CHECK_INTERVAL
        elapsed=$((elapsed + CHECK_INTERVAL))
    done

    if [ $MAX_WAIT_SECONDS -eq 0 ]; then
        echo "ERROR: Unexpected exit from infinite wait loop (this should not happen)"
    else
        echo "ERROR: Timeout waiting for controller to be ready after ${MAX_WAIT_SECONDS}s"
    fi
    return 1
}

# Add controller to existing quorum
add_controller_to_quorum() {
    echo "Adding controller to quorum..."

    # Get bootstrap controller
    local BOOTSTRAP_CONTROLLER=$(get_bootstrap_controller)
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to get bootstrap controller"
        return 1
    fi

    echo "Using bootstrap controller: ${BOOTSTRAP_CONTROLLER}"

    # TODO: https://t3.uberinternal.com/browse/DKAFC-6972
    # This is a temporary workaround from https://github.com/uber-code/data-kafka/pull/27/
    # The principal.builder.class causes issues with add-controller command
    local MODIFIED_CONFIG="/tmp/server.properties.$$"
    cp /etc/kafka/server.properties "${MODIFIED_CONFIG}"
    sed -i '/^principal.builder.class/d' "${MODIFIED_CONFIG}"

    # Add controller using kafka-metadata-quorum.sh
    local ADD_OUTPUT
    ADD_OUTPUT=$(unset JMX_PORT; unset KAFKA_JMX_OPTS; unset KAFKA_HEAP_OPTS; ${APP_HOME}/bin/kafka-metadata-quorum.sh \
        --bootstrap-controller "${BOOTSTRAP_CONTROLLER}" \
        --command-config "${MODIFIED_CONFIG}" \
        add-controller 2>&1)
    local EXIT_CODE=$?

    # Cleanup temporary config file
    rm -f "${MODIFIED_CONFIG}"

    # Check if controller was already added (this is not an error - idempotent operation)
    if echo "$ADD_OUTPUT" | grep -q "DuplicateVoterException"; then
        echo "INFO: Controller is already part of the quorum (already added previously)"
        echo "INFO: This is expected during restarts or retries - not an error"
        return 0
    fi

    # Check for normal success
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Successfully added controller to quorum"
        return 0
    else
        echo "ERROR: Failed to add controller to quorum"
        echo "ERROR: Command output: $ADD_OUTPUT" >&2
        return 1
    fi
}

join_controller_to_quorum_background() {
    echo "Background task: Waiting for controller to be ready (PID: $$)..."
    echo "Sleeping for ${CONTROLLER_STARTUP_DELAY_SECONDS} seconds before checking controller readiness..."
    sleep ${CONTROLLER_STARTUP_DELAY_SECONDS}

    wait_for_controller_ready
    if [ $? -eq 0 ]; then
        echo "Background task: Controller is ready, adding to quorum..."
        add_controller_to_quorum
        if [ $? -eq 0 ]; then
            echo "Background task: Controller successfully joined quorum"
        else
            echo "Background task: WARNING - Failed to add controller to quorum, but server is running"
        fi
    else
        echo "Background task: ERROR - Controller readiness check failed"
    fi
}

# Send M3 metric for monitoring controller readiness delays
# Args:
#   $1 - METRIC_NAME: metric name (e.g., "controller.readiness.delay")
#   $2 - METRIC_VALUE: metric value (e.g., 0 or 1)
# Returns:
#   Always returns 0 (fire-and-forget for observability)
# Notes:
#   - Runs in background to avoid blocking deployment
#   - Logs output to /var/log/kafka/m3_metrics.log
#   - Gracefully degrades if prerequisites are missing
send_m3_metric() {
    local METRIC_NAME="$1"
    local METRIC_VALUE="$2"

    # Validate parameters
    if [ -z "$METRIC_NAME" ] || [ -z "$METRIC_VALUE" ]; then
        echo "WARNING: send_m3_metric requires metric name and value" >&2
        echo "WARNING: Usage: send_m3_metric <metric_name> <metric_value>" >&2
        return 0  # Fire-and-forget: don't block deployment
    fi

    # Extract node ID from server.properties (KRaft mode uses node.id)
    local NODE_ID=$(grep "^node.id=" /etc/kafka/server.properties 2>/dev/null | cut -d'=' -f2 | tr -d '[:space:]')

    # Fallback to broker.id for ZooKeeper mode compatibility
    if [ -z "$NODE_ID" ]; then
        NODE_ID=$(grep "^broker.id=" /etc/kafka/server.properties 2>/dev/null | cut -d'=' -f2 | tr -d '[:space:]')
    fi

    # Validate NODE_ID exists and is numeric
    if [ -z "$NODE_ID" ] || ! [[ "$NODE_ID" =~ ^[0-9]+$ ]]; then
        echo "WARNING: Could not extract valid node.id from /etc/kafka/server.properties" >&2
        return 0  # Fire-and-forget: don't block deployment
    fi

    # Verify TOOL_PATH is set (indicates vars.sh was sourced)
    if [ -z "$TOOL_PATH" ]; then
        echo "WARNING: TOOL_PATH not set - ensure vars.sh is sourced before calling send_m3_metric" >&2
        return 0  # Fire-and-forget: don't block deployment
    fi

    # Verify run.py exists
    if [ ! -f "${TOOL_PATH}/run.py" ]; then
        echo "WARNING: ${TOOL_PATH}/run.py not found - cannot send M3 metric" >&2
        return 0  # Fire-and-forget: don't block deployment
    fi

    # Build metric command
    local METRIC_COMMAND="python3 ${TOOL_PATH}/run.py send-m3-metric --target-broker-id=${NODE_ID} --metric-name=${METRIC_NAME} --metric-value=${METRIC_VALUE}"

    # Always run in background to avoid blocking deployment
    # Redirect all output to dedicated log file
    ${METRIC_COMMAND} >>/var/log/kafka/m3_metrics.log 2>&1 &
    echo "INFO: M3 metric sent in background: ${METRIC_NAME}=${METRIC_VALUE}, node_id=${NODE_ID}" >&2

    # Always return success (fire-and-forget for observability)
    return 0
}
