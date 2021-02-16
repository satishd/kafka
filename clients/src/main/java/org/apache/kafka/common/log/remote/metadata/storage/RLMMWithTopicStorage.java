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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.log.remote.storage.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.log.remote.metadata.storage.RemoteLogMetadataSerdes.*;

/**
 * This is an implementation of {@link RemoteLogMetadataManager} based on internal topic storage.
 * <p>
 * This may be moved to core module as it is part of cluster running on brokers.
 */
public class RLMMWithTopicStorage implements RemoteLogMetadataManager {

    private static final Logger log = LoggerFactory.getLogger(RLMMWithTopicStorage.class);

    public static final String REMOTE_LOG_METADATA_TOPIC_NAME = Topic.REMOTE_LOG_METADATA_TOPIC_NAME;
    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP =
            "remote.log.metadata.topic.replication.factor";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP = "remote.log.metadata.topic.partitions";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP =
            "remote.log.metadata.topic.retention.ms";
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS = 50;
    public static final long DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS = 365 * 24 * 60 * 60 * 1000L;
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR = 3;

    private static final String COMMITTED_LOG_METADATA_FILE_NAME = "_rlmm_committed_metadata_log";

    private static final int PUBLISH_TIMEOUT_SECS = 120;
    public static final String REMOTE_LOG_METADATA_CLIENT_PREFIX = "__remote_log_metadata_client";

    private final RemoteLogMetadataSerdes serde = new RemoteLogMetadataSerdes();

    protected int noOfMetadataTopicPartitions;
    private ConcurrentMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata =
            new ConcurrentHashMap<>();
    private Map<TopicIdPartition, NavigableMap<Long, RemoteLogSegmentId>> partitionsWithOffsetSegmentIds =
            new ConcurrentHashMap<>();
    private Map<TopicIdPartition, ConcurrentMap<Integer, RemoteLogSegmentId>> partitionsWithLeaderEpochSegmentIds =
            new ConcurrentHashMap<>();
    private KafkaProducer<byte[], byte[]> producer;
    private AdminClient adminClient;
    private KafkaConsumer<byte[], byte[]> consumer;
    private String logDir;
    private volatile Map<String, ?> configs;

    private CommittedLogMetadataStore committedLogMetadataStore;
    private ConsumerTask consumerTask;
    private volatile boolean initialized;
    private boolean configured;

    private static class ProducerCallback implements Callback {
        private volatile RecordMetadata recordMetadata;

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;
            // exception is ignored as this would have already been thrown as we call Future<RecordMetadata>.get()
        }

        RecordMetadata recordMetadata() {
            return recordMetadata;
        }
    }

    @Override
    public synchronized void configure(Map<String, ?> configs) {
        if (configured) {
            log.info("configure is already invoked earlier.");
            return;
        }
        log.info("RLMMWithTopicStorage is initializing with configs: {}", configs);

        this.configs = Collections.unmodifiableMap(configs);

        Object propVal = configs.get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP);
        noOfMetadataTopicPartitions =
                (propVal == null) ? DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS : Integer.parseInt(propVal.toString());
        log.info("No of remote log metadata topic partitions: [{}]", noOfMetadataTopicPartitions);

        logDir = (String) configs.get("log.dir");
        if (logDir == null || logDir.trim().isEmpty()) {
            throw new IllegalArgumentException("log.dir can not be null or empty");
        }

        File metadataLogFile = new File(logDir, COMMITTED_LOG_METADATA_FILE_NAME);
        committedLogMetadataStore = new CommittedLogMetadataStore(metadataLogFile);

        configured = true;

        initializeResources();

        log.info("RLMMWithTopicStorage is initialized: {}", this);
    }

    private synchronized void initializeResources() {
        if (!initialized) {
            log.info("Initializing all the clients and resources.");

            if (configs.get(BOOTSTRAP_SERVERS_CONFIG) == null) {
                throw new InvalidConfigurationException("Broker endpoint must be configured for the remote log " +
                        "metadata manager.");
            }

            //create clients
            createAdminClient();
            createProducer();
            createConsumer();

            // todo-tier use rocksdb
            //load the stored data
            loadMetadataStore();

            initConsumerThread();

            initialized = true;
        }
    }

    private void loadMetadataStore() {
        try {
            final Collection<RemoteLogSegmentMetadata> remoteLogSegmentMetadatas = committedLogMetadataStore.read();
            for (RemoteLogSegmentMetadata entry : remoteLogSegmentMetadatas) {
                partitionsWithOffsetSegmentIds.computeIfAbsent(entry.remoteLogSegmentId().topicIdPartition(),
                        k -> new ConcurrentSkipListMap<>()).put(entry.startOffset(), entry.remoteLogSegmentId());
                partitionsWithLeaderEpochSegmentIds.computeIfAbsent(entry.remoteLogSegmentId().topicIdPartition(),
                        k -> new ConcurrentHashMap<>()).put(entry.leaderEpoch(), entry.remoteLogSegmentId());
                idWithSegmentMetadata.put(entry.remoteLogSegmentId(), entry);
            }
        } catch (IOException e) {
            throw new KafkaException("Error occurred while loading remote log metadata file.", e);
        }
    }

    private void ensureInitialized() {
        if (!initialized)
            throw new KafkaException("Resources required are not yet initialized by invoking initialize() method");
    }

    private void createAdminClient() {
        Map<String, Object> props = new HashMap<>(configs);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, createClientId("admin"));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        this.adminClient = AdminClient.create(props);
    }

    private void createProducer() {
        Map<String, Object> props = new HashMap<>(configs);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, createClientId("producer"));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    private String createClientId(String suffix) {
        // Added hashCode as part of client-id here to differentiate between multiple runs of broker.
        // Broker epoch could not be used as it is created only after RemoteLogManager and ReplicaManager are
        // created.
        return REMOTE_LOG_METADATA_CLIENT_PREFIX + "_" + suffix + configs.get("broker.id") + "_" + hashCode();
    }

    private void createConsumer() {
        Map<String, Object> props = new HashMap<>(configs);

        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, createClientId("consumer"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 30 * 1000);
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
    }

    private void initConsumerThread() {
        try {
            // start a thread to continuously consume records from topic partitions.
            consumerTask = new ConsumerTask(consumer, logDir, new RemotePartitionMetadataEventHandlerImpl(), serde);
            KafkaThread.daemon("RLMM-Consumer-Task", consumerTask).start();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error encountered while getting no of partitions of a remote log metadata " +
                    "topic with name : " + REMOTE_LOG_METADATA_TOPIC_NAME);
        }
    }

    @Override
    public void putRemoteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        ensureInitialized();

        // insert remote log metadata into the topic.
        log.info("Publishing messages to remote log metadata topic for remote log segment metadata [{}]",
                remoteLogSegmentMetadata);

        try {
            RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
            byte[] data = serde.serialize(new RemoteLogMetadataContext(REMOTE_LOG_SEGMENT_METADATA_API_KEY, (byte) 0,
                    remoteLogSegmentMetadata));
            doPublishMessageToPartition(remoteLogSegmentId.topicIdPartition(), data);
        } catch (KafkaException e) {
            String errorMsg = String.format("Received exception while adding remote log segment: %s", remoteLogSegmentMetadata);
            log.error(errorMsg);
            throw new RemoteStorageException(errorMsg, e);
        }
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) throws RemoteStorageException {
        ensureInitialized();

        // insert remote log metadata into the topic.
        try {
            RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadataUpdate.remoteLogSegmentId();
            byte[] data = serde.serialize(new RemoteLogMetadataContext(REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, (byte) 0,
                    remoteLogSegmentMetadataUpdate));
            doPublishMessageToPartition(remoteLogSegmentId.topicIdPartition(), data);
        } catch (KafkaException e) {
            String errorMsg = String.format("Received exception while updating remote log segment: %s", remoteLogSegmentMetadataUpdate);
            log.error(errorMsg);
            throw new RemoteStorageException(errorMsg, e);
        }

    }

    @Override
    public void updateRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) throws RemoteStorageException {
        try {
            byte[] data = serde.serialize(new RemoteLogMetadataContext(REMOTE_PARTITION_DELETE_API_KEY, (byte) 0,
                    remotePartitionDeleteMetadata));
            doPublishMessageToPartition(remotePartitionDeleteMetadata.topicIdPartition(), data);
        } catch (KafkaException e) {
            String errorMsg = String.format("Received exception while updating state for delete partition: %s", remotePartitionDeleteMetadata);
            log.error(errorMsg);
            throw new RemoteStorageException(errorMsg, e);
        }
    }

    private void doPublishMessageToPartition(TopicIdPartition topicIdPartition, byte[] data) throws KafkaException {
        int metadataPartitionNo = metadataPartitionFor(topicIdPartition);
        try {
            final ProducerCallback callback = new ProducerCallback();
            if (metadataPartitionNo >= noOfMetadataTopicPartitions) {
                log.error("Chosen partition no [{}] is more than the partition count: [{}]", metadataPartitionNo, noOfMetadataTopicPartitions);
            }
            producer.send(new ProducerRecord<byte[], byte[]>(REMOTE_LOG_METADATA_TOPIC_NAME, metadataPartitionNo,
                    null, data), callback)
                    .get(PUBLISH_TIMEOUT_SECS, TimeUnit.SECONDS);

            final RecordMetadata recordMetadata = callback.recordMetadata();
            if (!recordMetadata.hasOffset()) {
                throw new KafkaException("Received record in the callback does not have offsets.");
            }

            waitTillConsumerCatchesUp(recordMetadata);
        } catch (ExecutionException e) {
            throw new KafkaException("Exception occurred while publishing message for topicIdPartition: "
                    + topicIdPartition, e.getCause());
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Exception occurred while publishing message for topicIdPartition: "
                    + topicIdPartition, e);
        }
    }

    private void waitTillConsumerCatchesUp(RecordMetadata recordMetadata) {
        final int partition = recordMetadata.partition();

        // if the current assignment does not have the subscription for this partition then return immediately.
        if (!consumerTask.assignedPartition(partition)) {
            log.warn("This consumer is not subscribed to the target partition [{}] on which message is produced.",
                    partition);
            return;
        }

        final long offset = recordMetadata.offset();
        final long sleepTimeMs = 1000L;
        while (consumerTask.committedOffset(partition) < offset) {
            log.debug("Did not receive the messages till the expected offset [{}] for partition [{}], Sleeping for [{}]",
                    offset, partition, sleepTimeMs);
            Utils.sleep(sleepTimeMs);
        }
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       long offset,
                                                                       int epochForOffset) throws RemoteStorageException {
        ensureInitialized();

        NavigableMap<Long, RemoteLogSegmentId> remoteLogSegmentIdMap = partitionsWithOffsetSegmentIds.get(topicIdPartition);
        if (remoteLogSegmentIdMap == null) {
            return Optional.empty();
        }

        // look for floor entry as the given offset may exist in this entry.
        Map.Entry<Long, RemoteLogSegmentId> entry = remoteLogSegmentIdMap.floorEntry(offset);
        if (entry == null) {
            // if the offset is lower than the minimum offset available in metadata then return null.
            return Optional.empty();
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = idWithSegmentMetadata.get(entry.getValue());
        // look forward for the segment which has the target offset, if it does not exist then return the highest
        // offset segment available.
        //todo-tier check for offset too.
        while (remoteLogSegmentMetadata != null && remoteLogSegmentMetadata.endOffset() < offset) {
            entry = remoteLogSegmentIdMap.higherEntry(entry.getKey());
            if (entry == null) {
                break;
            }
            remoteLogSegmentMetadata = idWithSegmentMetadata.get(entry.getValue());
        }

        return Optional.ofNullable(remoteLogSegmentMetadata);
    }

    public Optional<Long> highestLogOffset(TopicIdPartition topicIdPartition, int leaderEpoch) throws RemoteStorageException {
        ensureInitialized();

        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithOffsetSegmentIds.get(topicIdPartition);

        return map == null || map.isEmpty() ? Optional.empty() : Optional.of(map.lastEntry().getKey());
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicPartition) {
        return doListRemoteLogSegments(topicPartition, null);
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicPartition, long leaderEpoch) {
        return doListRemoteLogSegments(topicPartition, leaderEpoch);
    }

    public Iterator<RemoteLogSegmentMetadata> doListRemoteLogSegments(TopicIdPartition topicPartition, Long leaderEpoch) {
        ensureInitialized();

        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithOffsetSegmentIds.get(topicPartition);
        if (map == null) {
            return Collections.emptyIterator();
        }

        Stream<RemoteLogSegmentMetadata> metadataStream = map.values().stream()
                .filter(id -> idWithSegmentMetadata.get(id) != null)
                .map(remoteLogSegmentId -> idWithSegmentMetadata.get(remoteLogSegmentId));

        if (leaderEpoch != null)
            metadataStream = metadataStream.filter(metadata -> metadata.segmentLeaderEpochs().containsValue(leaderEpoch));

        return metadataStream.collect(Collectors.toList()).iterator();
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
        Objects.requireNonNull(leaderPartitions, "leaderPartitions can not be null");
        Objects.requireNonNull(followerPartitions, "followerPartitions can not be null");

        ensureInitialized();

        log.info("Received leadership notifications with leader partitions {} and follower partitions {}",
                leaderPartitions, followerPartitions);

        final HashSet<TopicIdPartition> allPartitions = new HashSet<>(leaderPartitions);
        allPartitions.addAll(followerPartitions);
        consumerTask.addAssignmentsForPartitions(allPartitions);
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
        ensureInitialized();

        // remove these partitions from the currently assigned topic partitions.
        consumerTask.removeAssignmentsForPartitions(partitions);
    }

    public int noOfMetadataTopicPartitions() {
        return noOfMetadataTopicPartitions;
    }

    public synchronized void doSyncLogMetadataDataFile() throws IOException {
        ensureInitialized();

        // idWithSegmentMetadata and partitionsWithSegmentIds are not going to be modified while this is being done.
        committedLogMetadataStore.write(idWithSegmentMetadata.values());
    }

    public void doHandleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata metadata) {
        ensureInitialized();
        TopicIdPartition tp = metadata.remoteLogSegmentId().topicIdPartition();
        final NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithOffsetSegmentIds
                .computeIfAbsent(tp, topicPartition -> new ConcurrentSkipListMap<>());
        if (metadata.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
            idWithSegmentMetadata.remove(metadata.remoteLogSegmentId());
            // todo-tier check for concurrent updates when leader/follower switches occur
            map.remove(metadata.startOffset());
        } else {
            map.put(metadata.startOffset(), metadata.remoteLogSegmentId());
            idWithSegmentMetadata.put(metadata.remoteLogSegmentId(), metadata);
        }
    }

    private void doHandleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate rlsmUpdate) {
        ensureInitialized();

        RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
        RemoteLogSegmentMetadata metadata = idWithSegmentMetadata.get(remoteLogSegmentId);
        if(metadata == null) {
            throw new IllegalStateException(String.format("No remote log segment metadata found for: %s", remoteLogSegmentId));
        }

        RemoteLogSegmentMetadata updated = new RemoteLogSegmentMetadata(remoteLogSegmentId, metadata.startOffset(),
                metadata.endOffset(), metadata.maxTimestamp(), rlsmUpdate.leaderEpoch(), rlsmUpdate.eventTimestamp(),
                metadata.segmentSizeInBytes(), rlsmUpdate.state(), metadata.segmentLeaderEpochs());
        idWithSegmentMetadata.put(remoteLogSegmentId, updated);
    }

    private void doHandleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) {
        //todo-tier let partition remover handle this!!
        // RemotePartitionRemover will subscribe to the local tier metadata partitions and process them.
        NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithOffsetSegmentIds.remove(remotePartitionDeleteMetadata.topicIdPartition());
        if(map != null) {
            for (RemoteLogSegmentId id : map.values()) {
                idWithSegmentMetadata.remove(id);
            }
        }
    }

    public int metadataPartitionFor(TopicIdPartition tp) {
        ensureInitialized();
        Objects.requireNonNull(tp, "TopicPartition can not be null");

        int partitionNo = Utils.toPositive(Utils.murmur2(tp.toString().getBytes(StandardCharsets.UTF_8))) % noOfMetadataTopicPartitions;
        log.debug("No of partitions [{}], partitionNo: [{}] for given topic: [{}]", noOfMetadataTopicPartitions, partitionNo, tp);
        return partitionNo;
    }

    @Override
    public void close() throws IOException {
        // close resources
        try {
            if (producer != null) producer.close(Duration.ofSeconds(60));
        } catch (Exception ex) { /* ignore */ }
        try {
            if (consumer != null) consumer.close(Duration.ofSeconds(60));
        } catch (Exception ex) { /* ignore */ }
        try {
            if (adminClient != null) adminClient.close(Duration.ofSeconds(60));
        } catch (Exception ex) { /* ignore */ }

        Utils.closeQuietly(consumerTask, "ConsumerTask");
        idWithSegmentMetadata = new ConcurrentHashMap<>();
        partitionsWithOffsetSegmentIds = new ConcurrentHashMap<>();
    }

    private class RemotePartitionMetadataEventHandlerImpl implements RemotePartitionMetadataEventHandler {

        @Override
        public void handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            doHandleRemoteLogSegmentMetadata(remoteLogSegmentMetadata);
        }

        @Override
        public void handleRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) {
            doHandleRemoteLogSegmentMetadataUpdate(remoteLogSegmentMetadataUpdate);
        }

        @Override
        public void handleRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata) {
            doHandleRemotePartitionDeleteMetadata(remotePartitionDeleteMetadata);
        }

        @Override
        public void syncLogMetadataDataFile() throws IOException {
            doSyncLogMetadataDataFile();
        }

        @Override
        public int metadataPartitionFor(TopicIdPartition tp) {
            return RLMMWithTopicStorage.this.metadataPartitionFor(tp);
        }
    }

}
