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

# Copy the offline-rebuild logic to a shared path which can be used by kafka container
# OFFLINE_REBUILD_ROOT_PATH_OVERRIDE is same as SHARED_RSYNC_PYTHON3_PATH/partition_moving_tools
rm -rf ${OFFLINE_REBUILD_ROOT_PATH_OVERRIDE} /shared/rsync && mkdir -p ${SHARED_RSYNC_PYTHON3_PATH}
cp -r ${OFFLINE_REBUILD_ROOT_PATH} ${SHARED_RSYNC_PYTHON3_PATH}

# By convention, can hardcode RSYNC_DIR=/data1/data. but better set by the odin-kafka-worker
RSYNC_DIR=${RSYNC_DIR:-/data1/data}

echo "replace the rsync directory with the environment variable from odin-kafka-worker container"
sed -i -E "s#RSYNC_DIR#$RSYNC_DIR#g" ${APP_HOME}/udeploy/config/rsyncd.conf

exec /usr/bin/rsync --no-detach --daemon --port ${RSYNC_PORT} --config ${APP_HOME}/udeploy/config/rsyncd.conf
