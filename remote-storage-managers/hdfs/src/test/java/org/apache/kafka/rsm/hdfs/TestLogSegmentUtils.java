package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.common.log.remote.storage.LogSegmentData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.LEADER_EPOCH_FILE_NAME;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.LOG_FILE_NAME;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.OFFSET_INDEX_FILE_NAME;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.PRODUCER_SNAPSHOT_FILE_NAME;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.TIME_INDEX_FILE_NAME;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.TXN_INDEX_FILE_NAME;

public class TestLogSegmentUtils {

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
