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
package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.common.log.remote.storage.LogSegmentData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestLogSegmentUtils {

    public static final String LOG_FILE_NAME = "log";
    public static final String OFFSET_INDEX_FILE_NAME = "index";
    public static final String TIME_INDEX_FILE_NAME = "time";
    public static final String LEADER_EPOCH_FILE_NAME = "leader-epoch-checkpoint";
    public static final String TXN_INDEX_FILE_NAME = "txn";
    public static final String PRODUCER_SNAPSHOT_FILE_NAME = "snapshot";

    public static LogSegmentData createLogSegmentData(File logDir,
                                                      int startOffset,
                                                      int segSize,
                                                      boolean withOptionalFiles) throws IOException {
        String prefix = String.format("%020d", startOffset);
        File segment = new File(logDir, prefix + "." + LOG_FILE_NAME);
        Files.write(segment.toPath(), kafka.utils.TestUtils.randomBytes(segSize));

        File offsetIndex = new File(logDir, prefix + "." + OFFSET_INDEX_FILE_NAME);
        Files.write(offsetIndex.toPath(), kafka.utils.TestUtils.randomBytes(10));

        File timeIndex = new File(logDir, prefix + "." + TIME_INDEX_FILE_NAME);
        Files.write(timeIndex.toPath(), kafka.utils.TestUtils.randomBytes(10));

        File leaderEpochIndex = new File(logDir, prefix + "." + LEADER_EPOCH_FILE_NAME);
        Files.write(leaderEpochIndex.toPath(), kafka.utils.TestUtils.randomBytes(10));

        File txnIndex = null;
        File producerSnapshotIndex = null;
        if (withOptionalFiles) {
            txnIndex = new File(logDir, prefix + "." + TXN_INDEX_FILE_NAME);
            producerSnapshotIndex = new File(logDir, prefix + "." + PRODUCER_SNAPSHOT_FILE_NAME);
            Files.write(txnIndex.toPath(), kafka.utils.TestUtils.randomBytes(10));
            Files.write(producerSnapshotIndex.toPath(), kafka.utils.TestUtils.randomBytes(10));
        }
        return new LogSegmentData(segment, offsetIndex, timeIndex, txnIndex, producerSnapshotIndex, leaderEpochIndex);
    }
}
