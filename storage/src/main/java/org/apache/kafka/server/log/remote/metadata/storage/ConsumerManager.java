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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ConsumerManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerManager.class);
    private static final long CONSUME_RECHECK_INTERVAL_MS = 50L;

    private final TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final Time time;
    private final ConsumerTask consumerTask;
    private final Thread consumerTaskThread;

    public ConsumerManager(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig,
                           RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                           RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner,
                           Time time) {
        this.rlmmConfig = rlmmConfig;
        this.topicPartitioner = rlmmTopicPartitioner;
        this.time = time;

        //Create a task to consume messages and submit the respective events to RemotePartitionMetadataEventHandler.
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(rlmmConfig.consumerProperties());
        consumerTask = new ConsumerTask(consumer, remotePartitionMetadataEventHandler, topicPartitioner);
        consumerTaskThread = KafkaThread.daemon("RLMMConsumerTask", consumerTask);
    }

    public void startConsumerThread() {
        try {
            // Start a thread to continuously consume records from topic partitions.
            consumerTaskThread.start();
        } catch (Exception e) {
            throw new KafkaException("Error encountered while initializing and scheduling ConsumerTask thread", e);
        }
    }

    /**
     * Wait until the consumption reaches the offset of the metadata partition for the given {@code recordMetadata}.
     *
     * @param recordMetadata record metadata to be checked for consumption.
     */
    public void waitTillConsumptionCatchesUp(RecordMetadata recordMetadata) {
        final int partition = recordMetadata.partition();

        // If the current assignment does not have the subscription for this partition then return immediately.
        if (!assignedPartition(partition)) {
            log.warn("This consumer is not subscribed to the target partition [{}] on which message is produced.",
                    partition);
            return;
        }

        final long offset = recordMetadata.offset();
        long startTimeMillis = time.milliseconds();
        while (true) {
            long committedOffset = committedOffset(partition);
            if (committedOffset >= offset) {
                break;
            }

            log.debug("Committed offset [{}] for partition [{}], but the target offset: [{}],  Sleeping for [{}] to retry again",
                    offset, partition, committedOffset, CONSUME_RECHECK_INTERVAL_MS);

            if (time.milliseconds() - startTimeMillis > rlmmConfig.consumeWaitMs()) {
                log.warn("Committed offset for partition:[{}] is : [{}], but the target offset: [{}] ",
                        partition, committedOffset, offset);
            }

            time.sleep(CONSUME_RECHECK_INTERVAL_MS);
        }
    }

    private long committedOffset(int partition) {
        return consumerTask.receivedOffsetForPartition(partition).orElse(-1L);
    }

    private boolean assignedPartition(int partition) {
        return consumerTask.assignedPartition(partition);
    }

    @Override
    public void close() throws IOException {
        // Consumer task will close the task and it internally closes all the resources including the consumer.
        Utils.closeQuietly(consumerTask, "ConsumerTask");

        // Wait until the consumer thread finishes.
        try {
            consumerTaskThread.join();
        } catch (Exception e) {
            log.error("Encountered error while waiting for consumerTaskThread to finish.", e);
        }
    }

    public void addAssignmentsForPartitions(HashSet<TopicIdPartition> allPartitions) {
        consumerTask.addAssignmentsForPartitions(allPartitions);
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        consumerTask.removeAssignmentsForPartitions(partitions);
    }

    public Optional<Long> receivedOffsetForPartition(int metadataPartition) {
        return consumerTask.receivedOffsetForPartition(metadataPartition);
    }
}
