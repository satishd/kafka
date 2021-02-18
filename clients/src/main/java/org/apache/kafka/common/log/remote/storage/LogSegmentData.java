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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * This represents all the required data and indexes for a specific log segment that needs to be stored in the remote
 * storage. This is passed with {@link RemoteStorageManager#copyLogSegment(RemoteLogSegmentMetadata, LogSegmentData)}
 * while copying a specific log segment to the remote storage.
 */
@InterfaceStability.Evolving
public class LogSegmentData {

    private final File logSegment;
    private final File offsetIndex;
    private final File timeIndex;
    private final File txnIndex;
    private final File producerIdSnapshotIndex;
    private final ByteBuffer leaderEpochIndex;

    /**
     * Creates a LogSegmentData instance with data and indexes.
     *
     * @param logSegment              actual log segment file.
     * @param offsetIndex             offset index file
     * @param timeIndex               time index file
     * @param txnIndex                transaction index file
     * @param producerIdSnapshotIndex producer-id-snapshot until this segment
     * @param leaderEpochIndex        leader-epoch-index until this segment.
     */
    public LogSegmentData(File logSegment, File offsetIndex, File timeIndex, File txnIndex,
                          File producerIdSnapshotIndex, ByteBuffer leaderEpochIndex) {
        this.logSegment = logSegment;
        this.offsetIndex = offsetIndex;
        this.timeIndex = timeIndex;
        this.txnIndex = txnIndex;
        this.producerIdSnapshotIndex = producerIdSnapshotIndex;
        this.leaderEpochIndex = leaderEpochIndex;
    }

    /**
     * @return Log segment file of this segment.
     */
    public File logSegment() {
        return logSegment;
    }

    /**
     * @return Offset index file.
     */
    public File offsetIndex() {
        return offsetIndex;
    }

    /**
     * @return Time index file of this segment.
     */
    public File timeIndex() {
        return timeIndex;
    }

    /**
     * @return Transaction index file of this segment.
     */
    public File txnIndex() {
        return txnIndex;
    }

    /**
     * @return Producer-id snapshot file until this segment.
     */
    public File producerIdSnapshotIndex() {
        return producerIdSnapshotIndex;
    }

    /**
     * @return Leader epoch index until this segment.
     */
    public ByteBuffer leaderEpochIndex() {
        return leaderEpochIndex;
    }

    @Override
    public String toString() {
        return "LogSegmentData{" +
                "logSegment=" + logSegment +
                ", offsetIndex=" + offsetIndex +
                ", timeIndex=" + timeIndex +
                ", txnIndex=" + txnIndex +
                ", producerIdSnapshotIndex=" + producerIdSnapshotIndex +
                ", leaderEpochIndex=" + leaderEpochIndex +
                '}';
    }
}