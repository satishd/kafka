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

import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.lake.decode.FileDeadLetterSink;
import org.apache.kafka.lake.decode.HeatpipeAvroDecoder;
import org.apache.kafka.lake.decode.RecordDecoder;
import org.apache.kafka.lake.decode.SchemaClient;
import org.apache.kafka.lake.decode.SchemaClientProvider;
import org.apache.kafka.lake.decode.DeadLetterSink;
import org.apache.kafka.lake.offset.OffsetTracker;
import org.apache.kafka.lake.read.RsmProvider;
import org.apache.kafka.lake.read.SegmentReader;
import org.apache.kafka.lake.write.HudiSegmentWriter;
import org.apache.kafka.lake.write.HudiWriterConfig;
import org.apache.kafka.lake.write.RecordFieldExtractor;

import org.apache.hadoop.conf.Configuration;

import java.nio.file.Paths;

/**
 * Builds a {@link Pipeline} from configuration, wiring only the stages the config enables so a
 * partially-configured deployment degrades gracefully rather than failing at startup:
 * <ul>
 *   <li>no {@code remote.storage.manager.class.name} &rarr; discovery only;</li>
 *   <li>RSM set but decode/write not fully configured &rarr; fetch only (counts records);</li>
 *   <li>everything present (see {@link ConverterConfig#decodeAndWriteEnabled()}) &rarr; decode+write.</li>
 * </ul>
 */
public final class PipelineFactory {

    private PipelineFactory() {
    }

    public static Pipeline build(ConverterConfig config) {
        boolean readEnabled = !config.rsmClassName().trim().isEmpty();
        boolean decodeEnabled = config.decodeAndWriteEnabled();

        RsmProvider rsmProvider = readEnabled ? new RsmProvider(config) : null;
        SegmentReader reader = readEnabled ? SegmentReader.create(
                rsmProvider.storageManager(), config.readMode(), config.readBlockBytes(),
                config.readCacheDir()) : null;
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

    private static HudiSegmentWriter buildWriter(ConverterConfig config) {
        HudiWriterConfig writerConfig = new HudiWriterConfig(
                config.hudiTableBasePath(), config.hudiTableName(),
                config.hudiRecordKeyField(), config.hudiPartitionPathField());
        return new HudiSegmentWriter(writerConfig, new Configuration(),
                RecordFieldExtractor.forField(config.hudiRecordKeyField()),
                RecordFieldExtractor.forField(config.hudiPartitionPathField()));
    }
}
