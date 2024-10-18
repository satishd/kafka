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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.log.LogFileUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class DumpHDFSRemoteLogSegment {

    /**
     * This tool can be used to dump a remote log segment from HDFS.
     * <p>
     *     Usage: DumpHDFSRemoteLogSegment server.properties topic partition topicUuid baseOffset segmentUuid outputDir
     * </p>
     *
     * To compile it:
     *      [udocker@/home/udocker/odin-kafka/external/hdfs/libs #]javac -d . -cp .:*:../../../libs/* DumpHDFSRemoteLogSegment.java
     * To run the tool:
     *      [udocker@/home/udocker/odin-kafka/external/hdfs/libs #]java -Xmx512M -Dlog4j.configuration=file:/home/udocker/odin-kafka/config/tools-log4j.properties
     *      -Djava.security.krb5.conf=/etc/kafka/krb5.conf -cp .:/opt/hdfs/conf:/home/udocker/odin-kafka/libs/*:/home/udocker/odin-kafka/external/hdfs/libs/*
     *      org.apache.kafka.rsm.hdfs.tools.DumpHDFSRemoteLogSegment /etc/kafka/server.properties hp-motion-driver_app 0 B0phu66QT9qgJGaQ5nAFxw 15 3edfRvq4QhmBYx8hWTdPbQ /tmp/
     *
     * @param args                      The arguments to the tool
     * @throws IOException              If there is an error reading or writing the files
     * @throws RemoteStorageException   If there is an error fetching the remote log segment
     */
    public static void main(String[] args) throws IOException, RemoteStorageException {
        Namespace namespace = parseArguments(args);
        String serverPropsFilename = namespace.getString("config");
        String topic = namespace.getString("topic");
        int partition = namespace.getInt("partition");
        Uuid topicUuid = Uuid.fromString(namespace.getString("topic_uuid"));
        long baseOffset = namespace.getLong("base_offset");
        Uuid segmentUuid = Uuid.fromString(namespace.getString("segment_uuid"));
        String outputDir = namespace.get("output_dir").toString();

        TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, partition, topic);
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, baseOffset,
                -1, -1, -1, -1, -1, Collections.singletonMap(0, 0L));
        String filename = LogFileUtils.filenamePrefixFromOffset(baseOffset);

        Map<String, String> configs = Utils.propsToStringMap(Utils.loadProps(serverPropsFilename));
        String rsmConfigPrefix = configs.getOrDefault(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX);
        Map<String, String> rsmConfigs = configs.entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(rsmConfigPrefix))
                .collect(Collectors.toMap(e -> e.getKey().split(rsmConfigPrefix)[1], Map.Entry::getValue));
        rsmConfigs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, "1048576"); // 1 MB cacheLineSize
        rsmConfigs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_BYTES_PROP, "16777216"); // 16 MB cache
        System.out.println("Using configs " + rsmConfigs);
        try (HDFSRemoteStorageManager manager = new HDFSRemoteStorageManager()) {
            manager.configure(rsmConfigs);
            // Fetch all the indexes for a segment
            for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
                File indexFile = new File(outputDir, filename + getSuffix(indexType));
                System.out.println("Fetching index " + indexFile);
                try (InputStream inputStream = manager.fetchIndex(metadata, indexType)) {
                    // copy the stream to a file
                    Files.copy(inputStream, indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                } catch (RemoteStorageException e) {
                    System.err.println("Failed to fetch index " + indexFile + ": " + e.getMessage());
                }
            }
            // Fetch the log segment
            File segmentFile = new File(outputDir, filename + LogFileUtils.LOG_FILE_SUFFIX);
            System.out.println("Fetching segment " + segmentFile);
            try (InputStream stream = manager.fetchLogSegment(metadata, 0)) {
                Files.copy(stream, segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            System.out.println("Done");
        }
    }

    private static String getSuffix(RemoteStorageManager.IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return LogFileUtils.INDEX_FILE_SUFFIX;
            case TIMESTAMP:
                return LogFileUtils.TIME_INDEX_FILE_SUFFIX;
            case PRODUCER_SNAPSHOT:
                return LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX;
            case TRANSACTION:
                return LogFileUtils.TXN_INDEX_FILE_SUFFIX;
            case LEADER_EPOCH:
                return ".leader-epoch-checkpoint";
        }
        throw new IllegalArgumentException("Unknown index type " + indexType);
    }

    static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("kafka-dump-remote-log")
                .defaultHelp(true)
                .description("Tool to dump a remote log segment from HDFS.");
        parser.addArgument("--config")
                .type(String.class)
                .required(false)
                .setDefault("/etc/kafka/server.properties")
                .help("The server.properties file to use for the remote storage manager configs");
        parser.addArgument("--topic")
                .type(String.class)
                .required(true)
                .help("REQUIRED: The topic name");
        parser.addArgument("--partition")
                .type(Integer.class)
                .required(true)
                .help("REQUIRED: The partition id");
        parser.addArgument("--topic-uuid")
                .type(String.class)
                .required(true)
                .help("REQUIRED: The topic uuid");
        parser.addArgument("--base-offset")
                .type(Long.class)
                .required(true)
                .help("REQUIRED: The base offset of the segment");
        parser.addArgument("--segment-uuid")
                .type(String.class)
                .required(true)
                .help("REQUIRED: The segment uuid");
        parser.addArgument("--output-dir")
                .type(Arguments.fileType().verifyCanWrite())
                .required(false)
                .setDefault(new File("."))
                .help("The directory to write the segment and indexes to");
        return parser.parseArgsOrFail(args);
    }
}
