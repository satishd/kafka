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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DelayedRemoteListOffsetsMetrics {

    // Set the package name as "kafka.server" and the class name as "DelayedRemoteListOffsetsMetrics" to maintain backward compatibility.
    public final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "DelayedRemoteListOffsetsMetrics");

    public final Meter aggregateExpirationMeter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS);

    public final Map<TopicPartition, Meter> partitionExpirationMeters = new ConcurrentHashMap<>();

    private Function<TopicPartition, Meter> topicPartitionMeterFunction = key -> {
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", key.topic());
        tags.put("partition", String.valueOf(key.partition()));
        return metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags);
    };

    public DelayedRemoteListOffsetsMetrics() {
    }

    public void recordExpiration(TopicPartition partition) {
        aggregateExpirationMeter.mark();
        partitionExpirationMeters.computeIfAbsent(partition, topicPartitionMeterFunction).mark();
    }
}