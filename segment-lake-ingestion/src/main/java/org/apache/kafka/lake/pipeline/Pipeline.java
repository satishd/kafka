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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns every per-run component (RSM, dead-letter sink, segment reader, decoder, Hudi writer, offset
 * tracker) and drives segments through a two-stage pipeline: a <b>concurrent decode stage</b>
 * (fetch + Avro-decode, up to {@code parallelism} segments at once) feeding a <b>single-writer
 * commit stage</b> (one thread that owns the {@link HudiSegmentWriter} and commits sequentially).
 *
 * <p>The single writer is required for correctness: {@code HoodieJavaWriteClient} is a single-writer
 * engine, so table init and commits must not run concurrently against one table. Decoded segments
 * are handed off over a bounded queue; a {@link Semaphore} bounds total in-flight work (decoding +
 * queued + committing) so discovery applies backpressure rather than exhausting memory.
 *
 * <p>Reliability: a failed commit is retried up to {@code maxRetries} times with backoff; on
 * exhaustion the segment is marked failed (its offset stays uncommitted so a restart re-processes it
 * idempotently) rather than silently dropped. {@link #isQuiescent()} reports when no segment is in
 * flight and none has failed, which the worker uses to decide when it is safe to commit offsets.
 *
 * <p>Which stages are wired depends on how completely the worker is configured (see
 * {@link PipelineFactory}): no RSM &rarr; discovery only; RSM but no decode/write target &rarr;
 * fetch only (counts records); fully configured &rarr; decode and write.
 */
public final class Pipeline implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private static final long WRITER_POLL_MS = 200L;

    private final RsmProvider rsmProvider;
    private final DeadLetterSink deadLetterSink;
    private final SegmentReader reader;
    private final RecordDecoder decoder;
    private final HudiSegmentWriter writer;
    private final OffsetTracker offsetTracker;
    private final ConverterMetrics metrics = new ConverterMetrics();

    private final int parallelism;
    private final int maxRetries;
    private final long retryBackoffMs;

    private final BlockingQueue<DecodedSegment> writeQueue;
    private final Semaphore inFlightPermits;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    private ExecutorService decodePool;
    private Thread writerThread;
    private volatile boolean writerRunning;

    // Test-convenience constructor: exercises decode()/writeSegment() directly without tuning the
    // async stage. parallelism/queue of 1, no retries.
    Pipeline(RsmProvider rsmProvider, DeadLetterSink deadLetterSink, SegmentReader reader,
             RecordDecoder decoder, HudiSegmentWriter writer, OffsetTracker offsetTracker) {
        this(rsmProvider, deadLetterSink, reader, decoder, writer, offsetTracker, 1, 1, 0, 0L);
    }

    Pipeline(RsmProvider rsmProvider, DeadLetterSink deadLetterSink, SegmentReader reader,
             RecordDecoder decoder, HudiSegmentWriter writer, OffsetTracker offsetTracker,
             int parallelism, int queueCapacity, int maxRetries, long retryBackoffMs) {
        this.rsmProvider = rsmProvider;
        this.deadLetterSink = deadLetterSink;
        this.reader = reader;
        this.decoder = decoder;
        this.writer = writer;
        this.offsetTracker = offsetTracker;
        this.parallelism = Math.max(1, parallelism);
        this.maxRetries = Math.max(0, maxRetries);
        this.retryBackoffMs = Math.max(0, retryBackoffMs);
        this.writeQueue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        // Bound total in-flight segments (decoding + queued + being committed) so process() blocks
        // discovery when saturated instead of buffering the whole topic in memory.
        this.inFlightPermits = new Semaphore(this.parallelism + Math.max(1, queueCapacity));
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

    /** Start the decode pool and single writer thread. Call once before {@link #process}. */
    public void start() {
        if (reader == null) {
            return; // discovery-only: no async stages needed.
        }
        this.decodePool = Executors.newFixedThreadPool(parallelism, namedFactory("segment-decode"));
        this.writerRunning = true;
        this.writerThread = new Thread(this::writerLoop, "segment-hudi-writer");
        this.writerThread.start();
    }

    /**
     * Submit a discovered segment for processing. Returns once the segment has been accepted for
     * decoding; decoding and committing happen asynchronously. Blocks the caller when the pipeline is
     * saturated (backpressure).
     */
    public void process(RemoteLogSegmentMetadata segment) throws InterruptedException {
        logDiscovered(segment);
        if (reader == null) {
            return;
        }
        inFlightPermits.acquire();
        inFlight.incrementAndGet();
        boolean submitted = false;
        try {
            decodePool.execute(() -> runDecode(segment));
            submitted = true;
        } finally {
            if (!submitted) {
                releaseInFlight();
            }
        }
    }

    private void runDecode(RemoteLogSegmentMetadata segment) {
        try {
            Optional<DecodedSegment> decoded = decode(segment);
            if (decoded.isPresent()) {
                writeQueue.put(decoded.get()); // writer thread finishes this segment
            } else {
                finish(true); // skipped or fetch-only: nothing to commit
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finish(false);
        } catch (IOException | RemoteStorageException | RuntimeException e) {
            LOG.error("Failed to fetch/decode segment {}", segment.remoteLogSegmentId(), e);
            finish(false);
        }
    }

    private void writerLoop() {
        while (writerRunning || !writeQueue.isEmpty()) {
            DecodedSegment segment;
            try {
                segment = writeQueue.poll(WRITER_POLL_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (segment == null) {
                continue;
            }
            finish(writeWithRetry(segment));
        }
    }

    /**
     * Skip / fetch-only / decode logic for one segment, run on a decode thread. Returns the decoded
     * segment to be committed, or empty when the segment was already processed (skipped) or the
     * pipeline is fetch-only (no decoder/writer configured).
     */
    Optional<DecodedSegment> decode(RemoteLogSegmentMetadata segment) throws IOException, RemoteStorageException {
        if (offsetTracker != null && offsetTracker.isProcessed(segment)) {
            LOG.info("Skipping already-processed segment {}", segment.remoteLogSegmentId());
            metrics.segmentSkipped();
            return Optional.empty();
        }
        if (decoder == null || writer == null) {
            long records = reader.countDataRecords(segment);
            LOG.info("Fetched segment {}: {} data records", segment.remoteLogSegmentId(), records);
            return Optional.empty();
        }

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
        return Optional.of(new DecodedSegment(
                segment, decodedBySchema, OffsetTracker.commitExtraMetadata(segment), deadLettered));
    }

    private boolean decodeInto(Map<Schema, List<GenericRecord>> decodedBySchema, String topic, Record record) {
        Optional<GenericRecord> decoded = decoder.decode(topic, record.offset(), record.value());
        decoded.ifPresent(inner -> decodedBySchema.computeIfAbsent(inner.getSchema(), s -> new ArrayList<>())
                .add(inner));
        return decoded.isPresent();
    }

    /**
     * Commit one decoded segment to Hudi and mark it processed. All schema groups are written before
     * the segment is marked processed, so a crash mid-segment does not record the segment as done
     * with only some groups landed. (Single-schema segments &mdash; the norm &mdash; are a single
     * atomic commit; a multi-schema segment is committed group-by-group and a retry after a partial
     * failure may re-write already-committed groups, i.e. at-least-once for that rare case.)
     *
     * <p>Throws on failure; the writer loop applies bounded retry. Package-private for tests.
     */
    void writeSegment(DecodedSegment decoded) {
        long start = System.currentTimeMillis();
        long written = 0;
        for (Map.Entry<Schema, List<GenericRecord>> entry : decoded.recordsBySchema().entrySet()) {
            writer.write(entry.getValue(), entry.getKey(), decoded.commitExtraMetadata());
            written += entry.getValue().size();
        }
        metrics.commitLatencyMs(System.currentTimeMillis() - start);
        metrics.recordsWritten(written);
        if (offsetTracker != null) {
            offsetTracker.markProcessed(decoded.segment());
        }
        metrics.segmentProcessed();
        LOG.info("Processed segment {}: {} record(s) written, {} dead-lettered",
                decoded.segment().remoteLogSegmentId(), written, decoded.deadLettered());
    }

    private boolean writeWithRetry(DecodedSegment decoded) {
        int attempt = 0;
        while (true) {
            try {
                writeSegment(decoded);
                return true;
            } catch (RuntimeException e) {
                if (attempt >= maxRetries) {
                    LOG.error("Giving up on segment {} after {} attempt(s); leaving offset uncommitted "
                                    + "so it is retried on restart",
                            decoded.segment().remoteLogSegmentId(), attempt + 1, e);
                    return false;
                }
                attempt++;
                metrics.writeRetry();
                LOG.warn("Hudi write attempt {} failed for segment {}; retrying",
                        attempt, decoded.segment().remoteLogSegmentId(), e);
                if (!backoff(attempt)) {
                    return false;
                }
            }
        }
    }

    private boolean backoff(int attempt) {
        if (retryBackoffMs == 0) {
            return true;
        }
        try {
            Thread.sleep(retryBackoffMs * attempt);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void finish(boolean success) {
        if (!success) {
            failed.incrementAndGet();
            metrics.segmentFailed();
        }
        releaseInFlight();
    }

    private void releaseInFlight() {
        inFlight.decrementAndGet();
        inFlightPermits.release();
    }

    /** @return true when no segment is in flight and none has permanently failed. */
    public boolean isQuiescent() {
        return inFlight.get() == 0 && failed.get() == 0;
    }

    /** @return number of segments that permanently failed to write during this run. */
    public int failedCount() {
        return failed.get();
    }

    /** Wait until no segment is in flight (all decoded and committed or failed), or the timeout elapses. */
    public boolean drain(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (inFlight.get() > 0) {
            if (System.currentTimeMillis() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(Math.min(50L, Math.max(1L, timeoutMs)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    private void logDiscovered(RemoteLogSegmentMetadata segment) {
        LOG.info("Discovered finished segment {} offsets=[{}, {}] size={}B location={}",
                segment.remoteLogSegmentId(),
                segment.startOffset(),
                segment.endOffset(),
                segment.segmentSizeInBytes(),
                segment.customMetadata().map(Object::toString).orElse("<none>"));
    }

    private static ThreadFactory namedFactory(String prefix) {
        AtomicLong counter = new AtomicLong();
        return runnable -> new Thread(runnable, prefix + "-" + counter.getAndIncrement());
    }

    @Override
    public void close() throws IOException {
        writerRunning = false;
        if (decodePool != null) {
            decodePool.shutdown();
            awaitTermination(decodePool);
        }
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (writer != null) {
            writer.close();
        }
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

    private static void awaitTermination(ExecutorService pool) {
        try {
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
