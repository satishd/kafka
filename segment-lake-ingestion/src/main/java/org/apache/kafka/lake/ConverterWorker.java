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

import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.lake.discovery.MetadataSource;
import org.apache.kafka.lake.discovery.TopicMetadataSource;
import org.apache.kafka.lake.pipeline.Pipeline;
import org.apache.kafka.lake.pipeline.PipelineFactory;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point for the segment-lake converter worker.
 *
 * <p>Tails the remote log metadata topic and hands each segment that reaches
 * {@code COPY_SEGMENT_FINISHED} to a {@link Pipeline} (built by {@link PipelineFactory}), which
 * concurrently fetches and decodes it and commits it through a single writer thread.
 *
 * <p>This class only owns the process lifecycle: argument parsing, config loading, the discovery
 * loop, and manual offset commits. Concurrency and per-segment logic live in {@link Pipeline}.
 *
 * <p>Offsets are committed manually and only at a <b>quiescent checkpoint</b> — when no segment is in
 * flight, none has failed, and none is half-assembled. At such a point the consumer's read position
 * is safe to resume from, so a segment whose write failed is re-processed on restart (idempotently,
 * via the Hudi commit timeline) rather than being silently skipped. A persistently failing segment
 * therefore holds offset progress until it is resolved, surfaced via the {@code segmentsFailed}
 * metric and error logs.
 *
 * <p>A segment stuck in {@code COPY_SEGMENT_STARTED} (its finalizing update never arrives — e.g. a
 * leader crash mid-copy) would otherwise leak memory and hold offsets forever. {@link #reclaimAbandoned}
 * reclaims such entries: it audits and drops segments deleted before they finished
 * ({@code segmentsAbortedByDelete}), and evicts segments pending longer than
 * {@code pending.segment.timeout.ms} ({@code segmentsEvicted}). Both are surfaced via metrics, WARN
 * logs, and a dead-letter audit row, so offset progress bounds itself instead of stalling.
 */
public final class ConverterWorker {

    private static final Logger LOG = LoggerFactory.getLogger(ConverterWorker.class);
    private static final long METRICS_LOG_INTERVAL_MS = 60_000L;
    private static final long DRAIN_TIMEOUT_MS = 120_000L;

    private ConverterWorker() {
    }

    public static void main(String[] args) throws Exception {
        Namespace ns = parseArgs(args);
        ConverterConfig config = new ConverterConfig(loadProperties(ns.getString("config")));

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "converter-shutdown"));

        try (Pipeline pipeline = PipelineFactory.build(config)) {
            pipeline.start();
            LOG.info("Starting segment-lake converter; discovering finished segments from topic {} "
                            + "(read={}, decodeAndWrite={})",
                    config.metadataTopic(), pipeline.readEnabled(), pipeline.decodeEnabled());
            runLoop(config, pipeline, running);
        }
        LOG.info("Segment-lake converter stopped");
    }

    private static void runLoop(ConverterConfig config, Pipeline pipeline, AtomicBoolean running) throws Exception {
        long commitIntervalMs = config.offsetCommitIntervalMs();
        try (MetadataSource source = new TopicMetadataSource(config)) {
            long lastCommitMs = System.currentTimeMillis();
            long lastSummaryLogMs = System.currentTimeMillis();
            while (running.get()) {
                for (RemoteLogSegmentMetadata segment : source.poll()) {
                    pipeline.process(segment);
                }
                long now = System.currentTimeMillis();
                if (now - lastCommitMs >= commitIntervalMs) {
                    reclaimAbandoned(source, pipeline, config);
                    maybeCommit(source, pipeline);
                    lastCommitMs = now;
                }
                lastSummaryLogMs = maybeLogSummary(pipeline, lastSummaryLogMs);
            }
            // Drain in-flight work, then commit whatever is now safe before shutting down.
            if (!pipeline.drain(DRAIN_TIMEOUT_MS)) {
                LOG.warn("Timed out draining in-flight segments during shutdown");
            }
            reclaimAbandoned(source, pipeline, config);
            maybeCommit(source, pipeline);
        }
        pipeline.metrics().logSummary();
    }

    /**
     * Reclaim segments stuck in COPY_SEGMENT_STARTED so they stop leaking memory and holding back
     * offset commits: audit + drop segments deleted before they finished, and evict segments pending
     * longer than the configured timeout. Both are surfaced (metric + WARN + dead-letter audit row).
     *
     * <p>Package-private for tests.
     */
    static void reclaimAbandoned(MetadataSource source, Pipeline pipeline, ConverterConfig config) {
        for (RemoteLogSegmentMetadata segment : source.drainAbandoned()) {
            LOG.warn("Segment {} ({}) was deleted before it finished; it was never ingested. Auditing and dropping",
                    segment.remoteLogSegmentId(), segment.topicIdPartition());
            pipeline.metrics().segmentAbortedByDelete();
            pipeline.auditAbandonedSegment(segment, "deleted before ingest");
        }
        long timeoutMs = config.pendingSegmentTimeoutMs();
        if (timeoutMs <= 0) {
            return;
        }
        for (RemoteLogSegmentMetadata segment : source.evictStale(timeoutMs)) {
            LOG.warn("Evicting segment {} ({}) still in COPY_SEGMENT_STARTED after exceeding the pending "
                            + "timeout of {}ms; treating it as abandoned so offset progress can resume",
                    segment.remoteLogSegmentId(), segment.topicIdPartition(), timeoutMs);
            pipeline.metrics().segmentEvicted();
            pipeline.auditAbandonedSegment(segment, "evicted after pending timeout " + timeoutMs + "ms");
        }
    }

    /**
     * Commit the consumer read position only if it is safe: nothing in flight or failed in the
     * pipeline, and nothing half-assembled in discovery. Otherwise the read position covers records
     * still needed on restart, so we hold the offset and try again at the next checkpoint.
     */
    private static void maybeCommit(MetadataSource source, Pipeline pipeline) {
        if (pipeline.isQuiescent() && source.pendingCount() == 0) {
            source.commit();
        } else if (pipeline.failedCount() > 0) {
            LOG.warn("Holding offset: {} segment(s) failed to write and will be retried on restart",
                    pipeline.failedCount());
        }
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
}
