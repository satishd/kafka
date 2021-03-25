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

import java.io.File;
import java.util.Objects;

public class LogSegmentData {

    private final File logSegment;
    private final File offsetIndex;
    private final File timeIndex;
    private final File txnIndex;
    private final File producerIdSnapshotIndex;
    private final File leaderEpochCheckpoint;

    public LogSegmentData(File logSegment, File offsetIndex, File timeIndex, File txnIndex, File producerIdSnapshotIndex,
                          File leaderEpochCheckpoint) {
        this.logSegment = logSegment;
        this.offsetIndex = offsetIndex;
        this.timeIndex = timeIndex;
        this.txnIndex = txnIndex;
        this.producerIdSnapshotIndex = producerIdSnapshotIndex;
        this.leaderEpochCheckpoint = leaderEpochCheckpoint;
    }

    public File logSegment() {
        return logSegment;
    }

    public File offsetIndex() {
        return offsetIndex;
    }

    public File timeIndex() {
        return timeIndex;
    }

    public File txnIndex() {
        return txnIndex;
    }

    public File producerIdSnapshotIndex() {
        return producerIdSnapshotIndex;
    }

    public File leaderEpochCheckpoint() {
        return leaderEpochCheckpoint;
    }

    @Override
    public String toString() {
        return "LogSegmentData{" +
                "logSegment=" + logSegment +
                ", offsetIndex=" + offsetIndex +
                ", timeIndex=" + timeIndex +
                ", txnIndex=" + txnIndex +
                ", producerIdSnapshotIndex=" + producerIdSnapshotIndex +
                ", leaderEpochCheckpoint=" + leaderEpochCheckpoint +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogSegmentData that = (LogSegmentData) o;
        return Objects.equals(logSegment, that.logSegment) && Objects.equals(offsetIndex, that.offsetIndex) && Objects.equals(timeIndex, that.timeIndex) && Objects.equals(txnIndex, that.txnIndex) && Objects.equals(producerIdSnapshotIndex, that.producerIdSnapshotIndex) && Objects.equals(leaderEpochCheckpoint, that.leaderEpochCheckpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logSegment, offsetIndex, timeIndex, txnIndex, producerIdSnapshotIndex, leaderEpochCheckpoint);
    }
}
