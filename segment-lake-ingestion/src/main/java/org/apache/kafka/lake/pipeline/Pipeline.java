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
package org.apache.kafka.lake.pipeline;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.lake.decode.DeadLetterSink;
import org.apache.kafka.lake.decode.RecordDecoder;
import org.apache.kafka.lake.metrics.ConverterMetrics;
import org.apache.kafka.lake.offset.OffsetTracker;
import org.apache.kafka.lake.read.RsmProvider;
import org.apache.kafka.lake.read.SegmentReader;
import org.apache.kafka.lake.write.HudiSegmentWriter;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Owns every per-run component (RSM, dead-letter sink, segment reader, decoder, Hudi writer, offset
 * tracker) and drives a single segment through skip &rarr; fetch &rarr; decode &rarr; write.
 *
 * <p>Which stages are wired depends on how completely the worker is configured (see
 * {@link PipelineFactory}): with no RSM it is discovery-only; with an RSM but no decode/write target
 * it is fetch-only (counts records); fully configured it decodes and writes.
 *
 * <p>Safe to share across the worker's segment-processing threads: each {@link #process} call only
 * touches per-segment local state plus the thread-safe {@link ConverterMetrics} counters and
 * {@link OffsetTracker} (both of which support concurrent access).
 */
public final class Pipeline implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    private final RsmProvider rsmProvider;
    private final DeadLetterSink deadLetterSink;
    private final SegmentReader reader;
    private final RecordDecoder decoder;
    private final HudiSegmentWriter writer;
    private final OffsetTracker offsetTracker;
    private final ConverterMetrics metrics = new ConverterMetrics();

    Pipeline(RsmProvider rsmProvider, DeadLetterSink deadLetterSink, SegmentReader reader,
             RecordDecoder decoder, HudiSegmentWriter writer, OffsetTracker offsetTracker) {
        this.rsmProvider = rsmProvider;
        this.deadLetterSink = deadLetterSink;
        this.reader = reader;
        this.decoder = decoder;
        this.writer = writer;
        this.offsetTracker = offsetTracker;
    }

    public boolean readEnabled() {
        return reader != null;
    }

    public boolean decodeEnabled() {
        return decoder != null;
    }

    public ConverterMetrics metrics() {
        return metrics;
    }

    public void process(RemoteLogSegmentMetadata segment) {
        logDiscovered(segment);
        if (reader == null) {
            return;
        }
        if (offsetTracker != null && offsetTracker.isProcessed(segment)) {
            LOG.info("Skipping already-processed segment {}", segment.remoteLogSegmentId());
            metrics.segmentSkipped();
            return;
        }
        try {
            if (decoder == null || writer == null) {
                long records = reader.countDataRecords(segment);
                LOG.info("Fetched segment {}: {} data records", segment.remoteLogSegmentId(), records);
                return;
            }
            decodeAndWrite(segment);
        } catch (IOException | RemoteStorageException | RuntimeException e) {
            metrics.segmentFailed();
            LOG.error("Failed to process segment {}", segment.remoteLogSegmentId(), e);
        }
    }

    private void decodeAndWrite(RemoteLogSegmentMetadata segment) throws IOException, RemoteStorageException {
        String topic = segment.topicIdPartition().topic();
        Map<Schema, List<GenericRecord>> decodedBySchema = new LinkedHashMap<>();
        long deadLettered = 0;
        try (CloseableIterator<RecordBatch> batches = reader.batches(segment)) {
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
                if (batch.isControlBatch()) {
                    continue;
                }
                for (Record record : batch) {
                    deadLettered += decodeInto(decodedBySchema, topic, record) ? 0 : 1;
                }
            }
        }
        metrics.recordsDeadLettered(deadLettered);

        long start = System.currentTimeMillis();
        long written = writeAll(decodedBySchema, OffsetTracker.commitExtraMetadata(segment));
        metrics.commitLatencyMs(System.currentTimeMillis() - start);
        metrics.recordsWritten(written);

        if (offsetTracker != null) {
            offsetTracker.markProcessed(segment);
        }
        metrics.segmentProcessed();
        LOG.info("Processed segment {}: {} record(s) written, {} dead-lettered",
                segment.remoteLogSegmentId(), written, deadLettered);
    }

    private boolean decodeInto(Map<Schema, List<GenericRecord>> decodedBySchema, String topic, Record record) {
        Optional<GenericRecord> decoded = decoder.decode(topic, record.offset(), record.value());
        decoded.ifPresent(inner -> decodedBySchema.computeIfAbsent(inner.getSchema(), s -> new ArrayList<>())
                .add(inner));
        return decoded.isPresent();
    }

    private long writeAll(Map<Schema, List<GenericRecord>> decodedBySchema, Map<String, String> commitExtraMetadata) {
        long written = 0;
        for (Map.Entry<Schema, List<GenericRecord>> entry : decodedBySchema.entrySet()) {
            writer.write(entry.getValue(), entry.getKey(), commitExtraMetadata);
            written += entry.getValue().size();
        }
        return written;
    }

    private void logDiscovered(RemoteLogSegmentMetadata segment) {
        LOG.info("Discovered finished segment {} offsets=[{}, {}] size={}B location={}",
                segment.remoteLogSegmentId(),
                segment.startOffset(),
                segment.endOffset(),
                segment.segmentSizeInBytes(),
                segment.customMetadata().map(Object::toString).orElse("<none>"));
    }

    @Override
    public void close() throws IOException {
        if (deadLetterSink != null) {
            deadLetterSink.close();
        }
        if (reader != null) {
            reader.close();
        }
        if (rsmProvider != null) {
            rsmProvider.close();
        }
    }
}
