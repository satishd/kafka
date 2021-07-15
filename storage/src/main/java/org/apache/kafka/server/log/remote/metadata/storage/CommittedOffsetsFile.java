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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.server.common.SnapshotFile;
import org.apache.kafka.server.common.CheckpointFileFormatter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class represents a file containing the committed offsets of remote log metadata partitions. This will be used
 * by the default {@link org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager} to store consumed
 * offsets of the remote log metadata topic.
 */
public class CommittedOffsetsFile {
    private static final int CURRENT_VERSION = 0;

    private static final CheckpointFileFormatter<PartitionOffsetEntry> FORMATTER = new CheckpointFileFormatter<PartitionOffsetEntry>() {
        @Override
        public String toLine(PartitionOffsetEntry entry) {
            return entry.partition + " " + entry.offset;
        }

        @Override
        public Optional<PartitionOffsetEntry> fromLine(String line) {
            String[] strings = line.split(" ");
            if (strings.length != 2) {
                return Optional.empty();
            }

            return Optional.of(new PartitionOffsetEntry(Integer.parseInt(strings[0]), Long.parseLong(strings[1])));
        }
    };

    private final SnapshotFile<PartitionOffsetEntry> delegate;

    public CommittedOffsetsFile(File offsetsFile) {
        delegate = new SnapshotFile<>(offsetsFile, CURRENT_VERSION, FORMATTER);
    }

    public Map<Integer, Long> readEntries() throws IOException {
        List<PartitionOffsetEntry> entriesList = delegate.read();
        Map<Integer, Long> result = new HashMap<>(entriesList.size());
        for (PartitionOffsetEntry entry : entriesList) {
            result.put(entry.partition, entry.offset);
        }

        return result;
    }

    public void writeEntries(Map<Integer, Long> partitionToConsumedOffsets) throws IOException {
        List<PartitionOffsetEntry> result = new ArrayList<>(partitionToConsumedOffsets.size());
        for (Map.Entry<Integer, Long> entry : partitionToConsumedOffsets.entrySet()) {
            result.add(new PartitionOffsetEntry(entry.getKey(), entry.getValue()));
        }

        delegate.write(result);
    }

    public static class PartitionOffsetEntry {
        private final int partition;
        private final long offset;

        private PartitionOffsetEntry(int partition,
                                     long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
}