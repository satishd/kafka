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
 * The RemoteLogStorageTopicOffboarder is a utility for disabling remote log storage for Kafka topics.
 * It ensures that the specified topics are offboarded from remote storage by updating their configurations.
 *
 * <p>Usage:</p>
 * <pre>
 * java -cp . org.apache.kafka.tools.RemoteLogStorageTopicOffboarder --bootstrap-server &lt;bootstrap-servers&gt; [--batch-size &lt;batch-size&gt;] [--batch-interval &lt;batch-interval&gt;] [--request-timeout &lt;request-timeout&gt;] [--topic-file &lt;topic-file&gt;]
 * </pre>
 *
 * <p>Arguments:</p>
 * <ul>
 *   <li><b>--bootstrap-server</b>: A comma-separated list of host:port pairs to use for establishing the initial connection to the Kafka cluster.</li>
 *   <li><b>--batch-size</b> (optional): Number of topics to update the configs in one batch. Default is 100.</li>
 *   <li><b>--batch-interval</b> (optional): The amount of time to wait before executing the next batch. Timeout value is in milliseconds. Default is 900000 ms (15 minutes).</li>
 *   <li><b>--request-timeout</b> (optional): Admin client request timeout in milliseconds. Default is 120000 ms (2 minutes).</li>
 *   <li><b>--topic-file</b> (optional): Path to a file containing a list of topic names to be offboarded from remote storage.</li>
 * </ul>
 *
 * <p>Example:</p>
 * <pre>
 * java -cp . org.apache.kafka.tools.RemoteLogStorageTopicOffboarder --bootstrap-server localhost:9092 --batch-size 50 --batch-interval 600000 --request-timeout 180000 --topic-file topics.txt
 * </pre>
 *
 * <p>This tool performs the following steps:</p>
 * <ol>
 *   <li>Reads the list of topics from the specified file or retrieves all topics from the cluster.</li>
 *   <li>Filters the topics to identify those eligible for offboarding from remote storage.</li>
 *   <li>Updates the configuration of eligible topics to disable remote log storage in batches.</li>
 * </ol>
 *
 */
public class RemoteLogStorageTopicOffboarder extends RemoteLogStorageTopicExecutor {

    public RemoteLogStorageTopicOffboarder(Namespace namespace) throws IOException {
        super(namespace);
    }

    @Override
    boolean isEligibleTopic(ConfigEntry cleanupPolicyConfig, ConfigEntry remoteStorageConfig) {
        return cleanupPolicyConfig.value().toLowerCase(Locale.getDefault()).equals(TopicConfig.CLEANUP_POLICY_DELETE)
                && Boolean.parseBoolean(remoteStorageConfig.value());
    }

    @Override
    Collection<AlterConfigOp> alterConfigOps() {
        return Collections.singletonList(
                new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), AlterConfigOp.OpType.SET));
    }

    public static void main(String[] args) throws IOException {
        Namespace namespace = parseArguments(args);
        RemoteLogStorageTopicOffboarder offboarder = new RemoteLogStorageTopicOffboarder(namespace);
        offboarder.execute();
    }
}
