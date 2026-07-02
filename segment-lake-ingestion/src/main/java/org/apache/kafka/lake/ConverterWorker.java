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
import org.apache.kafka.lake.locate.RsmProvider;
import org.apache.kafka.lake.read.SegmentReader;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point for the segment-lake converter worker.
 *
 * <p>This stage tails the remote log metadata topic and, for each segment that reaches
 * {@code COPY_SEGMENT_FINISHED}, fetches it through the configured {@link RsmProvider} and counts
 * its data records to validate the read path. When no RemoteStorageManager is configured it logs
 * discovery only. Decode and Hudi write are added by later commits.
 */
public final class ConverterWorker {

    private static final Logger LOG = LoggerFactory.getLogger(ConverterWorker.class);

    private ConverterWorker() {
    }

    public static void main(String[] args) throws Exception {
        Namespace ns = parseArgs(args);
        Properties props = loadProperties(ns.getString("config"));
        ConverterConfig config = new ConverterConfig(props);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "converter-shutdown"));

        boolean readEnabled = !config.rsmClassName().trim().isEmpty();
        LOG.info("Starting segment-lake converter; discovering finished segments from topic {} (read={})",
                config.metadataTopic(), readEnabled);

        RsmProvider rsmProvider = readEnabled ? new RsmProvider(config) : null;
        try {
            SegmentReader reader = readEnabled ? new SegmentReader(rsmProvider.storageManager()) : null;
            try (MetadataSource source = new TopicMetadataSource(config)) {
                while (running.get()) {
                    List<RemoteLogSegmentMetadata> segments = source.poll();
                    for (RemoteLogSegmentMetadata segment : segments) {
                        process(segment, reader);
                    }
                }
            }
        } finally {
            if (rsmProvider != null) {
                rsmProvider.close();
            }
        }
        LOG.info("Segment-lake converter stopped");
    }

    private static void process(RemoteLogSegmentMetadata segment, SegmentReader reader) {
        LOG.info("Discovered finished segment {} offsets=[{}, {}] size={}B location={}",
                segment.remoteLogSegmentId(),
                segment.startOffset(),
                segment.endOffset(),
                segment.segmentSizeInBytes(),
                segment.customMetadata().map(Object::toString).orElse("<none>"));
        if (reader == null) {
            return;
        }
        try {
            long records = reader.countDataRecords(segment);
            LOG.info("Fetched segment {}: {} data records", segment.remoteLogSegmentId(), records);
        } catch (Exception e) {
            LOG.error("Failed to fetch segment {}", segment.remoteLogSegmentId(), e);
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("segment-lake-converter")
                .defaultHelp(true)
                .description("Convert tiered Kafka segments into a Hudi table (discovery stage).");
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
