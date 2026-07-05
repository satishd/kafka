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
package org.apache.kafka.lake.decode;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * Sink for records that could not be decoded, so a bad record dead-letters instead of stopping
 * ingestion of the rest of a segment.
 *
 * <p>The initial implementation ({@link FileDeadLetterSink}) writes to a local/mounted path.
 * Routing dead letters to an object-store path or a Kafka topic is future work (see the design
 * doc); the interface is kept narrow so a different sink can be swapped in via config without
 * touching {@link RecordDecoder} implementations.
 */
public interface DeadLetterSink extends Closeable {

    /**
     * Record one undecodable value.
     *
     * @param topic  the topic the record belongs to.
     * @param offset the record's offset within its partition.
     * @param value  the raw, undecoded record value.
     * @param reason human-readable reason the record was rejected.
     */
    void record(String topic, long offset, ByteBuffer value, String reason);

    /**
     * Record one abandoned segment (never ingested) for audit &mdash; e.g. a segment stuck in
     * COPY_SEGMENT_STARTED that was evicted after a timeout, or one deleted before it finished. This
     * is a segment-level audit trail, distinct from the per-record {@link #record} entries above.
     *
     * @param topicPartition the segment's topic-partition.
     * @param segmentId      the remote log segment id.
     * @param startOffset    the segment's start offset.
     * @param endOffset      the segment's end offset.
     * @param reason         human-readable reason the segment was abandoned.
     */
    void recordAbandonedSegment(String topicPartition, String segmentId, long startOffset, long endOffset,
                                String reason);
}
