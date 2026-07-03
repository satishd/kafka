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
package org.apache.kafka.lake.offset;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Makes ingestion idempotent by reconstructing, from the Hudi table's own commit timeline, which
 * remote log segments have already been committed.
 *
 * <p>Two independent skip conditions (either is sufficient):
 * <ul>
 *   <li>the segment's id has already been committed (survives replays of the exact same segment).</li>
 *   <li>the segment's {@code endOffset} is at or below the partition's committed high-water offset
 *       (covers segments that were re-created upstream but cover already-ingested offsets).</li>
 * </ul>
 *
 * <p>Not thread-safe; one instance per worker process, {@link #markProcessed} called from the same
 * thread that drives ingestion after each successful {@code HudiSegmentWriter.write}.
 */
public class OffsetTracker {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetTracker.class);

    public static final String TOPIC_ID_PARTITION_KEY = "kafka.topicIdPartition";
    public static final String SEGMENT_ID_KEY = "kafka.segmentId";
    public static final String START_OFFSET_KEY = "kafka.startOffset";
    public static final String END_OFFSET_KEY = "kafka.endOffset";
    public static final String LEADER_EPOCHS_KEY = "kafka.leaderEpochs";

    private final Set<String> processedSegmentIds;
    private final Map<String, Long> highWaterOffsets;

    private OffsetTracker(Set<String> processedSegmentIds, Map<String, Long> highWaterOffsets) {
        this.processedSegmentIds = processedSegmentIds;
        this.highWaterOffsets = highWaterOffsets;
    }

    /**
     * Reconstruct processed-segment state by scanning every completed commit's extra metadata.
     * Returns an empty tracker (nothing is skipped) if the table does not exist yet.
     */
    public static OffsetTracker load(Configuration hadoopConf, String tableBasePath) {
        Set<String> processedSegmentIds = new HashSet<>();
        Map<String, Long> highWaterOffsets = new HashMap<>();

        HoodieTableMetaClient metaClient;
        try {
            metaClient = HoodieTableMetaClient.builder()
                    .setConf(hadoopConf)
                    .setBasePath(tableBasePath)
                    .build();
        } catch (TableNotFoundException e) {
            LOG.info("No existing Hudi table at {}; starting with empty offset state", tableBasePath);
            return new OffsetTracker(processedSegmentIds, highWaterOffsets);
        }

        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
                .getInstants().forEach(instant -> apply(metaClient, instant, processedSegmentIds, highWaterOffsets));

        LOG.info("Loaded offset state from {}: {} processed segment(s), {} partition(s) with a high-water offset",
                tableBasePath, processedSegmentIds.size(), highWaterOffsets.size());
        return new OffsetTracker(processedSegmentIds, highWaterOffsets);
    }

    private static void apply(HoodieTableMetaClient metaClient, HoodieInstant instant,
                               Set<String> processedSegmentIds, Map<String, Long> highWaterOffsets) {
        Map<String, String> extra;
        try {
            byte[] details = metaClient.getActiveTimeline().getInstantDetails(instant).get();
            extra = HoodieCommitMetadata.fromBytes(details, HoodieCommitMetadata.class).getExtraMetadata();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read Hoodie commit metadata for instant " + instant.getTimestamp(), e);
        }

        String segmentId = extra.get(SEGMENT_ID_KEY);
        if (segmentId != null) {
            processedSegmentIds.add(segmentId);
        }
        String topicIdPartition = extra.get(TOPIC_ID_PARTITION_KEY);
        String endOffset = extra.get(END_OFFSET_KEY);
        if (topicIdPartition != null && endOffset != null) {
            highWaterOffsets.merge(topicIdPartition, Long.parseLong(endOffset), Math::max);
        }
    }

    /** @return true if {@code segment} has already been committed and should not be re-ingested. */
    public boolean isProcessed(RemoteLogSegmentMetadata segment) {
        if (processedSegmentIds.contains(segment.remoteLogSegmentId().id().toString())) {
            return true;
        }
        Long highWater = highWaterOffsets.get(segment.topicIdPartition().toString());
        return highWater != null && segment.endOffset() <= highWater;
    }

    /**
     * Update in-memory state after a segment's write has been committed, so later segments
     * discovered in the same run are also skipped without re-reading the Hudi timeline.
     */
    public void markProcessed(RemoteLogSegmentMetadata segment) {
        processedSegmentIds.add(segment.remoteLogSegmentId().id().toString());
        highWaterOffsets.merge(segment.topicIdPartition().toString(), segment.endOffset(), Math::max);
    }

    /** Build the commit extra-metadata a write for {@code segment} should be tagged with. */
    public static Map<String, String> commitExtraMetadata(RemoteLogSegmentMetadata segment) {
        Map<String, String> extra = new HashMap<>();
        extra.put(TOPIC_ID_PARTITION_KEY, segment.topicIdPartition().toString());
        extra.put(SEGMENT_ID_KEY, segment.remoteLogSegmentId().id().toString());
        extra.put(START_OFFSET_KEY, Long.toString(segment.startOffset()));
        extra.put(END_OFFSET_KEY, Long.toString(segment.endOffset()));
        extra.put(LEADER_EPOCHS_KEY, encodeLeaderEpochs(segment.segmentLeaderEpochs()));
        return extra;
    }

    private static String encodeLeaderEpochs(NavigableMap<Integer, Long> leaderEpochs) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : leaderEpochs.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(entry.getKey()).append(':').append(entry.getValue());
        }
        return sb.toString();
    }
}
