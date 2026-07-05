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

import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Reconstructs finished remote log segments from the stream of metadata records.
 *
 * <p>A segment is first published as a {@link RemoteLogSegmentMetadata} in state
 * {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}, and is only finalized later by a
 * {@link RemoteLogSegmentMetadataUpdate} carrying {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED}
 * together with the {@code customMetadata} (the remote bucket/location). This class merges the two
 * by {@link RemoteLogSegmentId} and emits a segment only once it is fully finalized.
 *
 * <p>The {@code pending} map holds started-but-not-finished segments; retaining the started base
 * record is unavoidable because the finalizing update alone lacks the segment's offsets/size/leader
 * epochs. A segment whose {@code FINISHED} never arrives (leader crash mid-copy, aborted copy, lost
 * update record) would otherwise stay pending forever &mdash; leaking memory and, because the worker
 * gates offset commits on {@link #pendingCount()}, stalling offset progress. Two mechanisms reclaim
 * such entries:
 * <ul>
 *   <li>{@link #drainAbandoned()} surfaces entries cleared by a {@code DELETE_*} record &mdash; the
 *       common leader-crash path, where the new leader's unreferenced-segment cleanup deletes the
 *       orphan;</li>
 *   <li>{@link #evictStale(long)} removes entries that have been pending longer than a timeout &mdash;
 *       the backstop for orphans that never receive a {@code DELETE_*}.</li>
 * </ul>
 *
 * <p>Not thread-safe; intended to be driven by a single consumer thread.
 */
public class SegmentAssembler {

    private final Map<RemoteLogSegmentId, PendingSegment> pending = new HashMap<>();
    /** Started segments cleared by a {@code DELETE_*} before ever being finished/ingested. */
    private final List<RemoteLogSegmentMetadata> abandonedByDelete = new ArrayList<>();
    /** Wall-clock source (millis) used only to age pending entries; overridable for tests. */
    private final LongSupplier clockMs;

    public SegmentAssembler() {
        this(System::currentTimeMillis);
    }

    public SegmentAssembler(LongSupplier clockMs) {
        this.clockMs = clockMs;
    }

    /**
     * Feed one metadata record and return any segments newly finalized by it.
     *
     * @param metadata a record read from the remote log metadata topic.
     * @return finished segments (usually empty or a single element).
     */
    public List<RemoteLogSegmentMetadata> accept(RemoteLogMetadata metadata) {
        if (metadata instanceof RemoteLogSegmentMetadata) {
            return acceptSegment((RemoteLogSegmentMetadata) metadata);
        } else if (metadata instanceof RemoteLogSegmentMetadataUpdate) {
            return acceptUpdate((RemoteLogSegmentMetadataUpdate) metadata);
        }
        // Partition-delete and snapshot records are not relevant to discovery.
        return Collections.emptyList();
    }

    private List<RemoteLogSegmentMetadata> acceptSegment(RemoteLogSegmentMetadata segment) {
        RemoteLogSegmentId id = segment.remoteLogSegmentId();
        switch (segment.state()) {
            case COPY_SEGMENT_STARTED:
                putPending(id, segment);
                return Collections.emptyList();
            case COPY_SEGMENT_FINISHED:
                // Already-finalized base record (rare): emit directly.
                pending.remove(id);
                return Collections.singletonList(segment);
            default:
                // DELETE_*: if we were still holding this started segment, it is being removed before
                // we ever ingested it; surface it for audit.
                captureAbandoned(id);
                return Collections.emptyList();
        }
    }

    private List<RemoteLogSegmentMetadata> acceptUpdate(RemoteLogSegmentMetadataUpdate update) {
        RemoteLogSegmentId id = update.remoteLogSegmentId();
        PendingSegment base = pending.get(id);
        if (base == null) {
            // The COPY_SEGMENT_STARTED record was not observed (e.g. consuming from a non-zero
            // offset, or a retention DELETE_* of a segment we already finished and dropped from
            // pending). Bootstrapping prior state from a snapshot/RLMM is handled in a later commit.
            return Collections.emptyList();
        }

        RemoteLogSegmentMetadata updated = base.metadata.createWithUpdates(update);
        switch (updated.state()) {
            case COPY_SEGMENT_FINISHED:
                pending.remove(id);
                return Collections.singletonList(updated);
            case COPY_SEGMENT_STARTED:
                putPending(id, updated);
                return Collections.emptyList();
            default:
                // DELETE_* for a still-pending (never finished) segment: reclaim and surface it.
                captureAbandoned(id);
                return Collections.emptyList();
        }
    }

    /** Insert or refresh a pending entry, preserving the original first-seen time on re-observation. */
    private void putPending(RemoteLogSegmentId id, RemoteLogSegmentMetadata metadata) {
        PendingSegment existing = pending.get(id);
        long firstSeenMs = existing != null ? existing.firstSeenMs : clockMs.getAsLong();
        pending.put(id, new PendingSegment(metadata, firstSeenMs));
    }

    /** Remove a pending entry cleared by a {@code DELETE_*} and record it for audit. */
    private void captureAbandoned(RemoteLogSegmentId id) {
        PendingSegment removed = pending.remove(id);
        if (removed != null) {
            abandonedByDelete.add(removed.metadata);
        }
    }

    /**
     * Remove pending segments that have been unresolved (neither finished nor deleted) for at least
     * {@code maxAgeMs} since first seen in this process, treating them as abandoned copies. This
     * bounds the pending map and lets offset progress resume past a segment whose finalizing update
     * will never arrive.
     *
     * @param maxAgeMs maximum time a segment may stay pending; {@code <= 0} disables eviction.
     * @return the evicted segments (empty if none), for the caller to count and audit.
     */
    public List<RemoteLogSegmentMetadata> evictStale(long maxAgeMs) {
        if (maxAgeMs <= 0 || pending.isEmpty()) {
            return Collections.emptyList();
        }
        long now = clockMs.getAsLong();
        List<RemoteLogSegmentMetadata> evicted = new ArrayList<>();
        Iterator<Map.Entry<RemoteLogSegmentId, PendingSegment>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            PendingSegment ps = it.next().getValue();
            if (now - ps.firstSeenMs >= maxAgeMs) {
                evicted.add(ps.metadata);
                it.remove();
            }
        }
        return evicted;
    }

    /**
     * @return started segments cleared by a {@code DELETE_*} since the previous call (i.e. deleted
     *         before they were ever finished/ingested), and clears the buffer.
     */
    public List<RemoteLogSegmentMetadata> drainAbandoned() {
        if (abandonedByDelete.isEmpty()) {
            return Collections.emptyList();
        }
        List<RemoteLogSegmentMetadata> drained = new ArrayList<>(abandonedByDelete);
        abandonedByDelete.clear();
        return drained;
    }

    /**
     * @return number of started-but-not-yet-finished segments currently tracked. Exposed for tests
     *         and observability.
     */
    public int pendingCount() {
        return pending.size();
    }

    /** A started segment awaiting finalization, tagged with when this process first saw it. */
    private static final class PendingSegment {
        private final RemoteLogSegmentMetadata metadata;
        private final long firstSeenMs;

        PendingSegment(RemoteLogSegmentMetadata metadata, long firstSeenMs) {
            this.metadata = metadata;
            this.firstSeenMs = firstSeenMs;
        }
    }
}
