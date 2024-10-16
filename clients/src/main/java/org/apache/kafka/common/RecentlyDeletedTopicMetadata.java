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

package org.apache.kafka.common;

import java.util.Properties;

/**
 * A class containing all the relevant Topic Metadata information needed to recreate the topic. The data
 * stored in ZK whenever the topic gets deleted and used to recreate the topic later. In case the ZK path
 * already exists, the old path is updated with the new data.
 */
public class RecentlyDeletedTopicMetadata {
    private final String topicName;
    private final int numPartitions;
    private final short replicationFactor;
    private final long deleteEpochTimestampMs;
    private final Properties configs;

    public RecentlyDeletedTopicMetadata(
            String topicName, int numPartitions, short replicationFactor,
            long deleteEpochTimestampMs, Properties configs) {
        this.topicName = topicName;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.deleteEpochTimestampMs = deleteEpochTimestampMs;
        this.configs = configs;
    }

    /**
     * The topic name
     */
    public String topicName() {
        return topicName;
    }

    /**
     * Number of partitions of the topic
     */
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * Replication Factor of the topic
     */
    public short replicationFactor() {
        return replicationFactor;
    }

    /**
     * Epoch Timestamp in Ms when the topic deletion happened.
     */
    public long deleteEpochTimestampMs() {
        return deleteEpochTimestampMs;
    }

    /**
     * List of properties used while creating the topic
     */
    public Properties configs() {
        return configs;
    }

    @Override
    public String toString() {
        return "TopicName: " + topicName +
                " (numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", deleteEpochTimestampMs=" + deleteEpochTimestampMs +
                ", configs=" + configs +
                ")";
    }
}
