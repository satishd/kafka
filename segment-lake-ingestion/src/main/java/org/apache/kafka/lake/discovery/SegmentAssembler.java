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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconstructs finished remote log segments from the stream of metadata records.
 *
 * <p>A segment is first published as a {@link RemoteLogSegmentMetadata} in state
 * {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}, and is only finalized later by a
 * {@link RemoteLogSegmentMetadataUpdate} carrying {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED}
 * together with the {@code customMetadata} (the remote bucket/location). This class merges the two
 * by {@link RemoteLogSegmentId} and emits a segment only once it is fully finalized.
 *
 * <p>Not thread-safe; intended to be driven by a single consumer thread.
 */
public class SegmentAssembler {

    private final Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> pending = new HashMap<>();

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
                pending.put(id, segment);
                return Collections.emptyList();
            case COPY_SEGMENT_FINISHED:
                // Already-finalized base record (rare): emit directly.
                pending.remove(id);
                return Collections.singletonList(segment);
            default:
                pending.remove(id);
                return Collections.emptyList();
        }
    }

    private List<RemoteLogSegmentMetadata> acceptUpdate(RemoteLogSegmentMetadataUpdate update) {
        RemoteLogSegmentId id = update.remoteLogSegmentId();
        RemoteLogSegmentMetadata base = pending.get(id);
        if (base == null) {
            // The COPY_SEGMENT_STARTED record was not observed (e.g. consuming from a non-zero
            // offset). Bootstrapping prior state from a snapshot/RLMM is handled in a later commit.
            return Collections.emptyList();
        }

        RemoteLogSegmentMetadata updated = base.createWithUpdates(update);
        switch (updated.state()) {
            case COPY_SEGMENT_FINISHED:
                pending.remove(id);
                return Collections.singletonList(updated);
            case COPY_SEGMENT_STARTED:
                pending.put(id, updated);
                return Collections.emptyList();
            default:
                pending.remove(id);
                return Collections.emptyList();
        }
    }

    /**
     * @return number of started-but-not-yet-finished segments currently tracked. Exposed for tests
     *         and observability.
     */
    public int pendingCount() {
        return pending.size();
    }
}
