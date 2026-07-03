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
package org.apache.kafka.lake.read;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import java.io.Closeable;
import java.io.IOException;

/**
 * A strategy for turning a remote segment into a stream of record batches. Implementations decide
 * how the bytes are obtained (streamed directly, prefetched to disk, ...) but all expose the same
 * batch-at-a-time view so callers are agnostic to the source.
 *
 * <p>Each {@link #batches(RemoteLogSegmentMetadata)} call returns an independent, closeable iterator
 * that owns its own per-segment resources; {@link #close()} releases any resources shared across
 * calls (most sources hold none).
 */
interface SegmentBatchSource extends Closeable {

    /**
     * Open the segment and stream its record batches one at a time. The caller <b>must</b> close the
     * returned iterator (ideally via try-with-resources) even if iteration is stopped early.
     */
    CloseableIterator<RecordBatch> batches(RemoteLogSegmentMetadata metadata)
            throws RemoteStorageException, IOException;

    @Override
    default void close() throws IOException {
    }
}
