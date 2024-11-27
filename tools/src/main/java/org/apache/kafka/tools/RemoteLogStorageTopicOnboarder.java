/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.TopicConfig;

import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * The RemoteLogStorageTopicOnboarder is a utility for enabling remote log storage for Kafka topics.
 * It ensures that the specified topics are onboarded to remote storage by updating their configurations.
 *
 * <p>Usage:</p>
 * <pre>
 * java -cp . org.apache.kafka.tools.RemoteLogStorageTopicOnboarder --bootstrap-server &lt;bootstrap-servers&gt; [--batch-size &lt;batch-size&gt;] [--batch-interval &lt;batch-interval&gt;] [--request-timeout &lt;request-timeout&gt;] [--topic-file &lt;topic-file&gt;]
 * </pre>
 *
 * <p>Arguments:</p>
 * <ul>
 *   <li><b>--bootstrap-server</b>: A comma-separated list of host:port pairs to use for establishing the initial connection to the Kafka cluster.</li>
 *   <li><b>--batch-size</b> (optional): Number of topics to update the configs in one batch. Default is 100.</li>
 *   <li><b>--batch-interval</b> (optional): The amount of time to wait before executing the next batch. Timeout value is in milliseconds. Default is 900000 ms (15 minutes).</li>
 *   <li><b>--request-timeout</b> (optional): Admin client request timeout in milliseconds. Default is 120000 ms (2 minutes).</li>
 *   <li><b>--topic-file</b> (optional): Path to a file containing a list of topic names to be onboarded to remote storage.</li>
 * </ul>
 *
 * <p>Example:</p>
 * <pre>
 * java -cp . org.apache.kafka.tools.RemoteLogStorageTopicOnboarder --bootstrap-server localhost:9092 --batch-size 50 --batch-interval 600000 --request-timeout 180000 --topic-file topics.txt
 * </pre>
 *
 * <p>This tool performs the following steps:</p>
 * <ol>
 *   <li>Reads the list of topics from the specified file or retrieves all topics from the cluster.</li>
 *   <li>Filters the topics to identify those eligible for onboarding to remote storage.</li>
 *   <li>Updates the configuration of eligible topics to enable remote log storage in batches.</li>
 * </ol>
 *
 * <p>Note: This tool requires the Kafka AdminClient to be properly configured and accessible.</p>
 */
public class RemoteLogStorageTopicOnboarder extends RemoteLogStorageTopicExecutor {

    public RemoteLogStorageTopicOnboarder(Namespace namespace) throws IOException {
        super(namespace);
    }

    /**
     * A topic is eligible to onboard to the remote storage when:
     *      1. `cleanup.policy` is set to delete and
     *      2. `remote.storage.enable` was not enabled currently (or)
     *      3. If `log.remote.storage.enable` is enabled at the broker level, then we also want to override the
     *         `remote.storage.enable` property at the topic level.
     * @param cleanupPolicyConfig   clean policy of the topic
     * @param remoteStorageConfig   remote storage config of the topic
     * @return True to denote that the topic needs to be enabled with remote storage
     */
    @Override
    boolean isEligibleTopic(ConfigEntry cleanupPolicyConfig, ConfigEntry remoteStorageConfig) {
        return cleanupPolicyConfig.value().toLowerCase(Locale.getDefault()).equals(TopicConfig.CLEANUP_POLICY_DELETE)
                && (!Boolean.parseBoolean(remoteStorageConfig.value()) ||
                        !remoteStorageConfig.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG));
    }

    @Override
    Collection<AlterConfigOp> alterConfigOps() {
        return Collections.singletonList(
                new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), AlterConfigOp.OpType.SET));
    }

    public static void main(String[] args) throws IOException {
        Namespace namespace = parseArguments(args);
        RemoteLogStorageTopicOnboarder onboarder = new RemoteLogStorageTopicOnboarder(namespace);
        onboarder.execute();
    }
}