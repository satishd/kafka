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
package org.apache.kafka.lake.decode;

import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/**
 * Appends one tab-separated line per dead-lettered record to a local (or mounted object-store)
 * path: {@code epochMillis\ttopic\toffset\treason\tbase64Value}.
 *
 * <p>Not safe for concurrent use from multiple processes against the same path; a single worker
 * instance is expected to own the file.
 */
public class FileDeadLetterSink implements DeadLetterSink {

    private static final Logger LOG = LoggerFactory.getLogger(FileDeadLetterSink.class);

    private final BufferedWriter writer;

    public FileDeadLetterSink(Path path) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open dead-letter file: " + path, e);
        }
    }

    @Override
    public synchronized void record(String topic, long offset, ByteBuffer value, String reason) {
        String encoded = Base64.getEncoder().encodeToString(Utils.toArray(value));
        String line = System.currentTimeMillis() + "\t" + topic + "\t" + offset + "\t"
                + reason.replace("\t", " ").replace("\n", " ") + "\t" + encoded;
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            LOG.error("Failed to write dead-letter record for {}-{}", topic, offset, e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}
