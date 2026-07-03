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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.lake.decode.RecordDecoder;
import org.apache.kafka.lake.offset.OffsetTracker;
import org.apache.kafka.lake.read.SegmentReader;
import org.apache.kafka.lake.write.HudiSegmentWriter;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

    @Test
    public void discoveryOnlyDoesNothingPerSegment() {
        Pipeline pipeline = new Pipeline(null, null, null, null, null, null);
        assertFalse(pipeline.readEnabled());
        assertFalse(pipeline.decodeEnabled());
        pipeline.process(segment()); // no reader: must not throw
    }

    @Test
    public void fetchOnlyCountsRecordsAndDoesNotDecode() throws Exception {
        SegmentReader reader = mock(SegmentReader.class);
        RemoteLogSegmentMetadata segment = segment();
        when(reader.countDataRecords(segment)).thenReturn(5L);

        Pipeline pipeline = new Pipeline(null, null, reader, null, null, null);
        assertTrue(pipeline.readEnabled());
        assertFalse(pipeline.decodeEnabled());
        pipeline.process(segment);

        verify(reader).countDataRecords(segment);
        verify(reader, never()).batches(any());
    }

    @Test
    public void skipsAlreadyProcessedSegment() throws Exception {
        SegmentReader reader = mock(SegmentReader.class);
        RecordDecoder decoder = mock(RecordDecoder.class);
        HudiSegmentWriter writer = mock(HudiSegmentWriter.class);
        OffsetTracker offsetTracker = mock(OffsetTracker.class);
        RemoteLogSegmentMetadata segment = segment();
        when(offsetTracker.isProcessed(segment)).thenReturn(true);

        Pipeline pipeline = new Pipeline(null, null, reader, decoder, writer, offsetTracker);
        pipeline.process(segment);

        verify(reader, never()).batches(any());
        verify(offsetTracker, never()).markProcessed(any());
    }

    @Test
    public void decodesWritesAndMarksProcessed() throws Exception {
        SegmentReader reader = mock(SegmentReader.class);
        RecordDecoder decoder = mock(RecordDecoder.class);
        HudiSegmentWriter writer = mock(HudiSegmentWriter.class);
        OffsetTracker offsetTracker = mock(OffsetTracker.class);
        RemoteLogSegmentMetadata segment = segment();

        when(offsetTracker.isProcessed(segment)).thenReturn(false);
        when(reader.batches(segment)).thenReturn(batchesOf("a", "b"));
        when(decoder.decode(eq("orders"), anyLong(), any())).thenReturn(Optional.of(record()));

        Pipeline pipeline = new Pipeline(null, null, reader, decoder, writer, offsetTracker);
        pipeline.process(segment);

        verify(decoder, times(2)).decode(eq("orders"), anyLong(), any());
        verify(writer).write(argThat(records -> records.size() == 2), eq(SCHEMA), any());
        verify(offsetTracker).markProcessed(segment);
    }

    @Test
    public void writeFailureIsSwallowedAndSegmentNotMarked() throws Exception {
        SegmentReader reader = mock(SegmentReader.class);
        RecordDecoder decoder = mock(RecordDecoder.class);
        HudiSegmentWriter writer = mock(HudiSegmentWriter.class);
        OffsetTracker offsetTracker = mock(OffsetTracker.class);
        RemoteLogSegmentMetadata segment = segment();

        when(offsetTracker.isProcessed(segment)).thenReturn(false);
        when(reader.batches(segment)).thenThrow(new RemoteStorageException("boom"));

        Pipeline pipeline = new Pipeline(null, null, reader, decoder, writer, offsetTracker);
        pipeline.process(segment); // must not propagate

        verify(offsetTracker, never()).markProcessed(any());
    }

    private static CloseableIterator<RecordBatch> batchesOf(String... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(values[i].getBytes());
        }
        MemoryRecords memoryRecords = MemoryRecords.withRecords(0L, Compression.NONE, records);
        List<RecordBatch> batches = new ArrayList<>();
        for (RecordBatch batch : memoryRecords.batches()) {
            batches.add(batch);
        }
        return CloseableIterator.wrap(batches.iterator());
    }

    private static GenericRecord record() {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", "x");
        return record;
    }

    private static RemoteLogSegmentMetadata segment() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), 0, "orders");
        return new RemoteLogSegmentMetadata(
                RemoteLogSegmentId.generateNew(tp), 0L, 9L, -1L, 1, 1000L, 1024,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                Collections.singletonMap(0, 0L));
    }
}
