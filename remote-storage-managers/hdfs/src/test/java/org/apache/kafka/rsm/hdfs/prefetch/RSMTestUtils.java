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

import org.apache.kafka.rsm.hdfs.LogSegmentDataHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class RSMTestUtils {

    public static void writeData(FileChannel fileChannel, byte[] data) throws IOException {
        // Create a ByteBuffer for the header
        ByteBuffer headerBuffer = ByteBuffer.allocate(LogSegmentDataHeader.LENGTH);
        // Set version byte
        headerBuffer.put(LogSegmentDataHeader.CURRENT_VERSION);

        // Calculate positions for all file types
        int startPos = LogSegmentDataHeader.LENGTH;
        int[] positions = new int[LogSegmentDataHeader.FileType.values().length];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = startPos;
            if (i < positions.length - 1) { // All except the last one (SEGMENT)
                startPos += 10; // Arbitrary size for non-segment files
            }
        }

        // Write positions to the header
        for (int pos : positions) {
            headerBuffer.putInt(pos);
        }

        // Reset buffer position for writing
        headerBuffer.flip();

        // Write the header to the file
        fileChannel.write(headerBuffer);

        // Write dummy data for non-segment files
        for (int i = 0; i < positions.length - 1; i++) {
            fileChannel.position(positions[i]);
            fileChannel.write(ByteBuffer.wrap(new byte[10])); // 10 bytes of zeros
        }

        // Write the test data at the SEGMENT position
        int segmentPos = positions[positions.length - 1]; // Position for SEGMENT
        fileChannel.position(segmentPos);
        fileChannel.write(ByteBuffer.wrap(data));
        fileChannel.position(0);
    }
}
