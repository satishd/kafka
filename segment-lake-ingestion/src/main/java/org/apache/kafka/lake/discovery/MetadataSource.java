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
package org.apache.kafka.lake.discovery;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

/**
 * Source of remote log segments that are finished being copied to remote storage and are therefore
 * safe to ingest into the lake.
 */
public interface MetadataSource extends Closeable {

    /**
     * Returns the set of segments newly observed to be in
     * {@link org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState#COPY_SEGMENT_FINISHED}
     * since the previous call. May be empty.
     *
     * @return finished segments discovered during this poll.
     */
    List<RemoteLogSegmentMetadata> poll();

    /**
     * @return number of segments seen started but not yet finished. While this is non-zero the
     *         current read position is <b>not</b> safe to commit, because reconstructing those
     *         segments on restart depends on records at or before the read position. This no longer
     *         stays non-zero forever for an abandoned segment: {@link #evictStale(long)} reclaims
     *         entries whose finalizing update never arrives, and {@link #drainAbandoned()} reclaims
     *         entries cleared by a {@code DELETE_*}.
     */
    default int pendingCount() {
        return 0;
    }

    /**
     * Evict segments that have been pending (started but neither finished nor deleted) longer than
     * {@code maxAgeMs}, treating them as abandoned so they stop leaking memory and holding back
     * offset commits.
     *
     * @param maxAgeMs maximum time a segment may stay pending; {@code <= 0} disables eviction.
     * @return the evicted segments, for the caller to count and audit. Empty by default.
     */
    default List<RemoteLogSegmentMetadata> evictStale(long maxAgeMs) {
        return Collections.emptyList();
    }

    /**
     * @return started segments cleared by a {@code DELETE_*} since the previous call (deleted before
     *         they were ever finished/ingested), for the caller to audit. Empty by default.
     */
    default List<RemoteLogSegmentMetadata> drainAbandoned() {
        return Collections.emptyList();
    }

    /**
     * Commit the current consumer read position. Callers must only invoke this at a quiescent
     * checkpoint (nothing in flight, none failed, {@link #pendingCount()} zero) so the committed
     * position is safe to resume from. No-op for sources without a committable offset.
     */
    default void commit() {
    }
}
