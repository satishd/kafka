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
set -e
shopt -s extglob

source udeploy/vars.sh

VERSION=$(echo ${GIT_DESCRIBE} |grep -oE 'uber/[0-9][^,]*' |sed 's;uber/;;' |sed 's;,;;')
echo "VERSION:" $VERSION

if [ -z $VERSION ]
then
    echo "Since there is no tag, we will use git sha1 as the version number"
    VERSION="0.0.$(echo ${GIT_COMMIT} | cut -c1-7)"
fi

sed -i 's/^version=.*$/version='"$VERSION"'/' gradle.properties

./gradlew releaseTarGz --stacktrace --no-daemon

cp core/build/distributions/!(*docs*) /tmp

# Remove unwanted except for the below
rm -rf !(udeploy)

# Symlinks
ln -s ${APP_HOME}/bin/zkcli.py /usr/local/bin/zkcli
ln -s ${APP_HOME}/udeploy/config /etc/kafka
rm -rf /etc/kafka/server.properties

tar -C ${APP_HOME} --strip 1 -xzf /tmp/kafka_*.tgz

pushd ${APP_HOME}/libs
curl -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar
popd

apt-get update && apt-get install -y --reinstall uber-data-hdfs-conf
mkdir -p ${HADOOP_CONF_DIR}
chown -R udocker:udocker ${HADOOP_CONF_DIR}
