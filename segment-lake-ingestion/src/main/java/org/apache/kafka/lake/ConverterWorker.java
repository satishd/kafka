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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point for the segment-lake converter worker.
 *
 * <p>Tails the remote log metadata topic and hands each segment that reaches
 * {@code COPY_SEGMENT_FINISHED} to a {@link Pipeline} (built by {@link PipelineFactory}), which
 * skips, fetches, decodes and writes it. Up to {@link ConverterConfig#maxConcurrentSegments()}
 * segments are processed concurrently.
 *
 * <p>This class only owns the process lifecycle: argument parsing, config loading, the discovery
 * loop, and the worker thread pool. All per-segment logic lives in {@link Pipeline}.
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

        try (Pipeline pipeline = PipelineFactory.build(config)) {
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
}
