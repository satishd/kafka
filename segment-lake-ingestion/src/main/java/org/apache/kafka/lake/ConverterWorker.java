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
package org.apache.kafka.lake;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.lake.decode.DeadLetterSink;
import org.apache.kafka.lake.decode.FileDeadLetterSink;
import org.apache.kafka.lake.decode.HeatpipeAvroDecoder;
import org.apache.kafka.lake.decode.RecordDecoder;
import org.apache.kafka.lake.decode.SchemaClient;
import org.apache.kafka.lake.decode.SchemaClientProvider;
import org.apache.kafka.lake.discovery.MetadataSource;
import org.apache.kafka.lake.discovery.TopicMetadataSource;
import org.apache.kafka.lake.locate.RsmProvider;
import org.apache.kafka.lake.metrics.ConverterMetrics;
import org.apache.kafka.lake.offset.OffsetTracker;
import org.apache.kafka.lake.read.SegmentReader;
import org.apache.kafka.lake.write.HudiSegmentWriter;
import org.apache.kafka.lake.write.HudiWriterConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point for the segment-lake converter worker.
 *
 * <p>Tails the remote log metadata topic and, for each segment that reaches
 * {@code COPY_SEGMENT_FINISHED}: skips it if {@link OffsetTracker} shows it is already committed,
 * fetches it through the configured {@code RemoteStorageManager}, decodes each record via
 * {@link HeatpipeAvroDecoder}, and writes the decoded batch to Hudi via {@link HudiSegmentWriter}.
 * Segments are processed with up to {@link ConverterConfig#maxConcurrentSegments()} in flight.
 *
 * <p>The full decode-and-write pipeline only runs once every required setting is present (see
 * {@link ConverterConfig#decodeAndWriteEnabled()}); otherwise the worker falls back to
 * discovery-only (or fetch-only, if just the RSM is configured), matching the behavior of earlier
 * commits so a partially-configured deployment fails safe rather than throwing at startup.
 */
public final class ConverterWorker {

    private static final Logger LOG = LoggerFactory.getLogger(ConverterWorker.class);
    private static final long METRICS_LOG_INTERVAL_MS = 60_000L;

    private ConverterWorker() {
    }

    public static void main(String[] args) throws Exception {
        Namespace ns = parseArgs(args);
        ConverterConfig config = new ConverterConfig(loadProperties(ns.getString("config")));

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "converter-shutdown"));

        try (Pipeline pipeline = Pipeline.build(config)) {
            LOG.info("Starting segment-lake converter; discovering finished segments from topic {} "
                            + "(read={}, decodeAndWrite={})",
                    config.metadataTopic(), pipeline.readEnabled(), pipeline.decodeEnabled());
            runLoop(config, pipeline, running);
        }
        LOG.info("Segment-lake converter stopped");
    }

    private static void runLoop(ConverterConfig config, Pipeline pipeline, AtomicBoolean running) throws Exception {
        int parallelism = Math.max(1, config.maxConcurrentSegments());
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        Semaphore inFlight = new Semaphore(parallelism);
        try (MetadataSource source = new TopicMetadataSource(config)) {
            long lastSummaryLogMs = System.currentTimeMillis();
            while (running.get()) {
                for (RemoteLogSegmentMetadata segment : source.poll()) {
                    inFlight.acquire();
                    executor.submit(() -> {
                        try {
                            pipeline.process(segment);
                        } finally {
                            inFlight.release();
                        }
                    });
                }
                lastSummaryLogMs = maybeLogSummary(pipeline, lastSummaryLogMs);
            }
            // Drain in-flight work before shutting down so no segment is left half-processed.
            inFlight.acquire(parallelism);
        } finally {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        pipeline.metrics().logSummary();
    }

    private static long maybeLogSummary(Pipeline pipeline, long lastSummaryLogMs) {
        long now = System.currentTimeMillis();
        if (now - lastSummaryLogMs > METRICS_LOG_INTERVAL_MS) {
            pipeline.metrics().logSummary();
            return now;
        }
        return lastSummaryLogMs;
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("segment-lake-converter")
                .defaultHelp(true)
                .description("Convert tiered Kafka segments into a Hudi table.");
        parser.addArgument("--config")
                .required(true)
                .help("Path to the converter properties file.");
        return parser.parseArgsOrFail(args);
    }

    private static Properties loadProperties(String path) throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(path)) {
            props.load(in);
        }
        return props;
    }

    /**
     * Owns every per-run component (RSM, schema client, dead-letter sink, Hudi writer, offset
     * tracker) and drives a single segment through skip &rarr; fetch &rarr; decode &rarr; write.
     * Safe to share across the worker's segment-processing threads: each {@link #process} call
     * only touches per-segment local state, plus the thread-safe {@link ConverterMetrics} counters
     * and {@link OffsetTracker} (whose mutation is confined to the single-writer discovery loop
     * that calls {@code markProcessed} - see {@code OffsetTracker}'s own not-thread-safe caveat,
     * which holds here because segments for a given partition are only ever discovered once).
     */
    private static final class Pipeline implements AutoCloseable {

        private final RsmProvider rsmProvider;
        private final DeadLetterSink deadLetterSink;
        private final SegmentReader reader;
        private final RecordDecoder decoder;
        private final HudiSegmentWriter writer;
        private final OffsetTracker offsetTracker;
        private final ConverterMetrics metrics = new ConverterMetrics();

        private Pipeline(RsmProvider rsmProvider, DeadLetterSink deadLetterSink, SegmentReader reader,
                          RecordDecoder decoder, HudiSegmentWriter writer, OffsetTracker offsetTracker) {
            this.rsmProvider = rsmProvider;
            this.deadLetterSink = deadLetterSink;
            this.reader = reader;
            this.decoder = decoder;
            this.writer = writer;
            this.offsetTracker = offsetTracker;
        }

        static Pipeline build(ConverterConfig config) {
            boolean readEnabled = !config.rsmClassName().trim().isEmpty();
            boolean decodeEnabled = config.decodeAndWriteEnabled();

            RsmProvider rsmProvider = readEnabled ? new RsmProvider(config) : null;
            SegmentReader reader = readEnabled ? new SegmentReader(rsmProvider.storageManager()) : null;
            if (!decodeEnabled) {
                return new Pipeline(rsmProvider, null, reader, null, null, null);
            }

            SchemaClient schemaClient = SchemaClientProvider.create(config);
            DeadLetterSink deadLetterSink = new FileDeadLetterSink(Paths.get(config.deadLetterPath()));
            RecordDecoder decoder = new HeatpipeAvroDecoder(schemaClient, deadLetterSink);
            HudiSegmentWriter writer = buildWriter(config);
            OffsetTracker offsetTracker = OffsetTracker.load(new Configuration(), config.hudiTableBasePath());
            return new Pipeline(rsmProvider, deadLetterSink, reader, decoder, writer, offsetTracker);
        }

        boolean readEnabled() {
            return reader != null;
        }

        boolean decodeEnabled() {
            return decoder != null;
        }

        ConverterMetrics metrics() {
            return metrics;
        }

        void process(RemoteLogSegmentMetadata segment) {
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
            for (RecordBatch batch : reader.fetch(segment).batches()) {
                if (batch.isControlBatch()) {
                    continue;
                }
                for (Record record : batch) {
                    deadLettered += decodeInto(decodedBySchema, topic, record) ? 0 : 1;
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

        private static HudiSegmentWriter buildWriter(ConverterConfig config) {
            HudiWriterConfig writerConfig = new HudiWriterConfig(
                    config.hudiTableBasePath(), config.hudiTableName(),
                    config.hudiRecordKeyField(), config.hudiPartitionPathField());
            return new HudiSegmentWriter(writerConfig, new Configuration(),
                    record -> stringField(record, config.hudiRecordKeyField()),
                    record -> stringField(record, config.hudiPartitionPathField()));
        }

        private static String stringField(GenericRecord record, String fieldName) {
            Object value = record.get(fieldName);
            if (value == null) {
                throw new IllegalStateException("Decoded record is missing required field '" + fieldName + "'");
            }
            return value.toString();
        }

        @Override
        public void close() throws IOException {
            if (deadLetterSink != null) {
                deadLetterSink.close();
            }
            if (rsmProvider != null) {
                rsmProvider.close();
            }
        }
    }
}
