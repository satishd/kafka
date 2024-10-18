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
package org.apache.kafka.rsm.hdfs.tools;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class HDFSStaleFileFinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSStaleFileFinder.class);
    private static final String KEYTAB_PATH = "/secrets/keytab/kloak.keytab";

    private final Configuration conf;

    public HDFSStaleFileFinder(String user, String defaultFsUri) throws IOException {
        conf = new Configuration();
        conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, defaultFsUri);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(user, KEYTAB_PATH);
    }

    private void listPathStatus(FileSystem fs,
                               Path path,
                               Duration maxRetentionTime,
                               AtomicInteger emptyDirectoriesCount,
                               AtomicInteger staleEmptyDirectoriesCount,
                               int level,
                               Map<Uuid, StaleTopic> staleTopicsMap) throws IOException {
        LOGGER.trace("Auditing Level: {}, Path: {}", level, path);
        if (level > 1) {
            LOGGER.warn("Auditing Level: {} is greater than 1. Path: {}", level, path);
            return;
        }
        // Root folder
        FileStatus[] fileStatuses = fs.listStatus(path);
        // traverse each partition folder
        for (FileStatus status : fileStatuses) {
            boolean isStale = status.getModificationTime() < System.currentTimeMillis() - maxRetentionTime.toMillis();
            Duration timeElapsedSinceUpdate = Duration.ofMillis(System.currentTimeMillis() - status.getModificationTime());
            // traverse one partition folder
            if (status.isDirectory()) {
                DirContentSummary contentSummary = dirContentSummary(fs, status.getPath());
                if (contentSummary.fileCount == 0) {
                    emptyDirectoriesCount.incrementAndGet();
                    if (isStale) {
                        staleEmptyDirectoriesCount.incrementAndGet();
                    }
                } else {
                    if (isStale) {
                        LOGGER.info("Directory: {} has not been modified for {} and contains {} files, size: {}",
                                status.getPath(),
                                formatDuration(timeElapsedSinceUpdate),
                                contentSummary.fileCount,
                                humanReadableByteCountBin(contentSummary.length));
                    }
                    listPathStatus(fs, status.getPath(), maxRetentionTime, emptyDirectoriesCount,
                            staleEmptyDirectoriesCount, level + 1, staleTopicsMap);
                }
            } else {
                // one segment file
                if (isStale) {
                    LOGGER.info("File: {} has not been modified for {} and size: {}",
                            status.getPath(),
                            formatDuration(timeElapsedSinceUpdate),
                            humanReadableByteCountBin(status.getLen()));

                    String filename = status.getPath().toString();
                    RemoteLogSegmentId remoteLogSegmentId = extractRemoteLogSegmentId(filename);
                    TopicIdPartition tpId = remoteLogSegmentId.topicIdPartition();

                    StaleTopic staleTopic = staleTopicsMap.computeIfAbsent(tpId.topicId(),
                        k -> new StaleTopic(tpId.topic(), tpId.topicId()));
                    staleTopic.segmentsByPartition.computeIfAbsent(tpId.partition(), k -> new HashSet<>())
                        .add(remoteLogSegmentId.id());
                    if (timeElapsedSinceUpdate.toMillis() > staleTopic.maxTimeElapsedSinceUpdate) {
                        staleTopic.maxTimeElapsedSinceUpdate = timeElapsedSinceUpdate.toMillis();
                    }
                }
            }
        }
    }

    private DirContentSummary dirContentSummary(FileSystem fs, Path path) throws IOException {
        int fileCount = 0;
        long length = 0;
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus status : fileStatuses) {
            // search only for files in the 1st level. Do not go deeper
            if (status.isFile()) {
                fileCount += 1;
                length += status.getLen();
            }
        }
        return new DirContentSummary(fileCount, length);
    }

    private void audit(String filePath, Duration maxRetentionTime) throws IOException {
        // Get the filesystem instance
        try (FileSystem fs = FileSystem.get(conf)) {
            // Path to the directory in HDFS
            long startTimeMs = System.currentTimeMillis();
            Path path = new Path(filePath);
            AtomicInteger emptyDirectoriesCount = new AtomicInteger();
            AtomicInteger staleEmptyDirectoriesCount = new AtomicInteger();
            Map<Uuid, StaleTopic> staleTopicsMap = new HashMap<>();
            listPathStatus(fs, path, maxRetentionTime, emptyDirectoriesCount, staleEmptyDirectoriesCount, 0, staleTopicsMap);

            LOGGER.info("====== SUMMARY ======");
            for (StaleTopic staleTopic : staleTopicsMap.values()) {
                LOGGER.info("Topic: {} has not been modified for {} and contains stale segment-count: {} in {} partitions. topic-id: {}",
                        staleTopic.topic,
                        formatDuration(Duration.ofMillis(staleTopic.maxTimeElapsedSinceUpdate)),
                        staleTopic.segmentsByPartition.entrySet()
                            .stream()
                            .reduce(0, (acc, entry) -> acc + entry.getValue().size(), Integer::sum),
                        staleTopic.segmentsByPartition.size(),
                        staleTopic.topicId);
            }
            LOGGER.info("Total number of empty directories: {}, stale: {}. Time taken: {} ms",
                    emptyDirectoriesCount.get(),
                    staleEmptyDirectoriesCount.get(),
                    System.currentTimeMillis() - startTimeMs);
        }
    }

    /**
     * <pre>
     * Before running this script, make sure the below items:
     *  1. Check the generated root folder name for each cluster (getRootFolderName)
     *  2. Check the generated max retention time for each cluster (getMaxRetentionTime)
     *  3. Check the generated defaultFsUri for each cluster (getDefaultFsUri)
     *  4. Check the user principal for the node where this script runs
     * Commands to run this class:
     *  1. javac -d . -cp  .:*:../external/hdfs/libs/* HDFSStaleFileFinder.java
     *  2. You can either generate the parameters or pass them as command line arguments
     *  {@code
     *  3. java -cp  .:*:../external/hdfs/libs/*:/opt/hdfs/conf -Djava.security.krb5.conf=/etc/kafka/krb5.conf \
     *     -Dlog4j.configuration=file:/home/udocker/odin-kafka/config/tools-log4j.properties \
     *     org/apache/kafka/rsm/hdfs/tools/HDFSStaleFileFinder d \
     *     kloak/kafka-staging1-dca.kafka-staging1-dca-bufif-labul.dca11-z59.prod.uber.internal \
     *     hdfs://ns-neon-prod-dca1 \
     *     hdfs://ns-neon-prod-dca1/user/kloak/kafka-remote-storage/kafka-d-dca PT50H > /tmp/d-dca.log
     *  4. java -cp  .:*:../external/hdfs/libs/*:/opt/hdfs/conf -Djava.security.krb5.conf=/etc/kafka/krb5.conf \
     *     -Dlog4j.configuration=file:/home/udocker/odin-kafka/config/tools-log4j.properties \
     *     org/apache/kafka/rsm/hdfs/tools/HDFSStaleFileFinder d > /tmp/d-dca.log
     *  }
     * </pre>
     * @param args Command line arguments
     * @throws IOException If an I/O error occurs
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            LOGGER.error("Usage: HDFSStaleFileFinder <cluster> <user> <defaultFsUri> <filePath> <maxRetentionTime>");
            return;
        }
        List<String> validClusters = Arrays.asList("arm", "staging1", "d", "logging", "logging2", "ingestion",
                "ingestion2", "dbevents1", "f", "a", "g", "agg1", "h");
        String cluster = args[0];
        if (cluster == null || cluster.isEmpty() || !validClusters.contains(cluster)) {
            LOGGER.error("Invalid cluster: {}", cluster);
            return;
        }
        String uberRegion = System.getenv("UBER_REGION");
        if (!("dca".equals(uberRegion) || "phx".equals(uberRegion))) {
            LOGGER.error("Invalid UBER_REGION: {}", uberRegion);
            return;
        }

        // Commented the below lines for reference
//        String user = "kloak/kafka-staging1-phx.kafka-staging1-phx-jumaz-husam.phx7-34p.prod.uber.internal";
//        String defaultFsUri = getDefaultFsUri(cluster, uberRegion);
//        String filePath = getRootFolderName(cluster, uberRegion, defaultFsUri);
//        Duration maxRetentionTime = getMaxRetentionTime(cluster);

        String user = args[1];
        String defaultFsUri = args[2];
        String filePath = args[3];
        Duration maxRetentionTime = Duration.parse(args[4]);
        LOGGER.info("user: {}, defaultFsUri: {}, filePath: {}, maxRetentionTime: {}",
                user, defaultFsUri, filePath, formatDuration(maxRetentionTime));
        HDFSStaleFileFinder hdfsStaleFileFinder = new HDFSStaleFileFinder(user, defaultFsUri);
        hdfsStaleFileFinder.audit(filePath, maxRetentionTime);
    }

    private static Duration getMaxRetentionTime(String cluster) {
        if (cluster.equals("arm")) {
            return Duration.ofHours(5);
        }
        if (cluster.equals("dbevents1")) {
            return Duration.ofDays(8);
        }
        return Duration.ofDays(3);
    }

    private static String getRootFolderName(String cluster, String uberRegion, String defaultFsUri) {
        if (cluster.equals("arm")) {
            return defaultFsUri + "/user/kloak/test-kafka-remote-storage/kafka-" + cluster + "-" + uberRegion;
        }
        return defaultFsUri + "/user/kloak/kafka-remote-storage/kafka-" + cluster + "-" + uberRegion;
    }

    private static String getDefaultFsUri(String cluster, String uberRegion) {
        String defaultFsUri;
        if (cluster.equals("arm") || cluster.equals("staging1") || cluster.equals("d") || cluster.equals("logging") || cluster.equals("logging2") || cluster.equals("ingestion")) {
            if (uberRegion.equals("dca")) {
                defaultFsUri = "hdfs://ns-neon-prod-dca1";
            } else {
                if (cluster.equals("ingestion")) {
                    defaultFsUri = "hdfs://ns-argon-prod-phx";
                } else {
                    defaultFsUri = "hdfs://ns-platinum-prod-phx";
                }
            }
        } else {
            if (uberRegion.equals("dca")) {
                defaultFsUri = "hdfs://ns-sulfur-prod-dca";
            } else {
                defaultFsUri = "hdfs://ns-kappa-prod-phx";
            }
        }
        return defaultFsUri;
    }

    private static RemoteLogSegmentId extractRemoteLogSegmentId(String filename) {
        String[] chunks = filename.split("/");
        Uuid segmentId = Uuid.fromString(chunks[chunks.length - 1]);
        String topicIdPartition = chunks[chunks.length - 2];
        Uuid topicId = Uuid.fromString(topicIdPartition.substring(topicIdPartition.length() - 22));
        String topicPartition = topicIdPartition.substring(0, topicIdPartition.length() - 23);
        String topic = topicPartition.substring(0, topicPartition.lastIndexOf("-"));
        int partition = Integer.parseInt(topicPartition.substring(topicPartition.lastIndexOf("-") + 1));
        TopicIdPartition tpId = new TopicIdPartition(topicId, partition, topic);
        return new RemoteLogSegmentId(tpId, segmentId);
    }

    // Helper method to convert bytes into a human-readable format
    private static String humanReadableByteCountBin(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        String[] units = {"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
        int i;
        for (i = 0; i < units.length && value >= 1024; i++) {
            value /= 1024;
        }
        value *= Long.signum(bytes);
        return String.format("%d %s", value, units[i - 1]);
    }

    private static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        // Calculate days, hours, minutes, and seconds
        long days = seconds / (24 * 3600);
        seconds %= 24 * 3600;
        long hours = seconds / 3600;
        seconds %= 3600;
        long minutes = seconds / 60;
        seconds %= 60;
        return String.format("%dd%dh%dm%ds", days, hours, minutes, seconds);
    }

    private static class StaleTopic {
        String topic;
        Uuid topicId;
        Map<Integer, Set<Uuid>> segmentsByPartition;
        long maxTimeElapsedSinceUpdate;

        private StaleTopic(String topic, Uuid topicId) {
            this.topic = topic;
            this.topicId = topicId;
            this.segmentsByPartition = new HashMap<>();
        }
    }

    private static class DirContentSummary {
        int fileCount;
        long length;

        private DirContentSummary(int fileCount, long length) {
            this.fileCount = fileCount;
            this.length = length;
        }
    }
}
