/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.common;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a utility to capture a snapshot in a file. It writes down to the file in the below format.
 *
 * ========= File beginning =========
 * version: int
 * entries-count: int
 * entry-as-string-on-each-line
 * ========= File end =========
 *
 * Each entry is represented as a string on each line in the snapshot file. {@link SnapshotFileEntryFormatter} is used
 * to convert the entry into a string and vice versa.
 *
 * @param <T> entry type.
 */
public class SnapshotFile<T> {

    private final int version;
    private final SnapshotFileEntryFormatter<T> formatter;
    private final Object lock = new Object();
    private final Path absolutePath;
    private final Path tempPath;

    public SnapshotFile(File file,
                        int version,
                        SnapshotFileEntryFormatter<T> formatter) {
        this.version = version;
        this.formatter = formatter;
        try {
            // Create the file if it does not exist.
            Files.createFile(file.toPath());
        } catch (FileAlreadyExistsException ex) {
            // Ignore if file already exists.
        } catch (IOException ex) {
            throw new KafkaException(ex);
        }
        absolutePath = file.toPath().toAbsolutePath();
        tempPath = Paths.get(absolutePath.toString() + ".tmp");
    }

    public void write(Collection<T> entries) throws IOException {
        synchronized (lock) {
            // write to temp file and then swap with the existing file
            try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
                // Write the version
                writer.write(Integer.toString(version));
                writer.newLine();

                // Write the entries count
                writer.write(Integer.toString(entries.size()));
                writer.newLine();

                // Write each entry on a new line.
                for (T entry : entries) {
                    writer.write(formatter.toString(entry));
                    writer.newLine();
                }

                writer.flush();
                fileOutputStream.getFD().sync();
            }

            Utils.atomicMoveWithFallback(tempPath, absolutePath);
        }
    }

    public List<T> read() throws IOException {
        synchronized (lock) {
            try (BufferedReader reader = Files.newBufferedReader(absolutePath)) {
                CheckpointReadBuffer<T> checkpointBuffer = new CheckpointReadBuffer<>(absolutePath.toString(), reader, version, formatter);
                return checkpointBuffer.read();
            }
        }
    }

    private static class CheckpointReadBuffer<T> {

        private final String location;
        private final BufferedReader reader;
        private final int version;
        private final SnapshotFileEntryFormatter<T> formatter;

        CheckpointReadBuffer(String location,
                             BufferedReader reader,
                             int version,
                             SnapshotFileEntryFormatter<T> formatter) {
            this.location = location;
            this.reader = reader;
            this.version = version;
            this.formatter = formatter;
        }

        List<T> read() throws IOException {
            String line = reader.readLine();
            if (line == null)
                return Collections.emptyList();

            int readVersion = toInt(line);
            if (readVersion != version) {
                throw new IOException();
            }

            line = reader.readLine();
            if (line == null) {
                return Collections.emptyList();
            }
            int expectedSize = toInt(line);
            List<T> entries = new ArrayList<>(expectedSize);
            line = reader.readLine();
            while (line != null) {
                Optional<T> maybeEntry = formatter.fromString(line);
                if (!maybeEntry.isPresent()) {
                    throw buildMalformedLineException(line);
                }
                entries.add(maybeEntry.get());
                line = reader.readLine();
            }

            if (entries.size() != expectedSize) {
                throw new IOException("Expected [" + expectedSize + "] entries in checkpoint file ["
                                              + line + "], but found only [" + entries.size() + "]");
            }
            return entries;
        }

        private int toInt(String line) throws IOException {
            try {
                return Integer.parseInt(line);
            } catch (NumberFormatException e) {
                throw buildMalformedLineException(line);
            }
        }

        private IOException buildMalformedLineException(String line) {
            return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line));
        }
    }
}
