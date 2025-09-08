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

package org.apache.kafka.rsm.hdfs.prefetch;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.InputStream;
import java.util.Map;

public interface RemoteDataPrefetcher extends Reconfigurable {

    void configure(Map<String, ?> configs);

    /**
     * Tells the prefetcher what the current segment and position being served are. This information is used
     * to determine whether some data for the partition should be prefetched.
     *
     * @param remoteLogSegmentMetadata  metadata of the remote log segment being read
     * @param currentPosition           the current position within the segment being served
     * @param nextSegmentOffsetAndEpoch the offset and epoch of the next segment to be read
     */
    void signalSegmentRead(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int currentPosition, OffsetAndEpoch nextSegmentOffsetAndEpoch);

    /**
     * Returns the InputStream for the specified log segment if it has already been prefetched.
     * If the log segment is not available in the prefetched data, this method returns null.
     *
     * @param remoteLogSegmentMetadata the metadata of the remote log segment being fetched
     * @param startPosition            the starting position within the log segment to fetch
     * @param endPosition              the ending position within the log segment to fetch
     * @return the InputStream for the requested log segment range if prefetched, otherwise null
     */
    InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition, int endPosition);

    /**
     * Cleans up any prefetched data that may have been loaded during the operation
     * of the prefetcher. This method is typically invoked as part of a resource
     * cleanup process, such as during the shutdown or disposal of the prefetcher
     * instance. It ensures that all temporary or cached resources are freed,
     * preventing resource leaks or excessive memory usage.
     */
    void cleanup();
}
