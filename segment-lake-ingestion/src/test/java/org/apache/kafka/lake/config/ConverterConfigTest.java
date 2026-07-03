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
package org.apache.kafka.lake.config;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.lake.read.ReadMode;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConverterConfigTest {

    private static Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConverterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }

    @Test
    public void readBlockBytesDefaultsTo4MiB() {
        ConverterConfig config = new ConverterConfig(baseProps());
        assertEquals(4 * 1024 * 1024, config.readBlockBytes());
    }

    @Test
    public void readBlockBytesHonorsOverride() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.READ_BLOCK_BYTES_CONFIG, 65536);
        assertEquals(65536, new ConverterConfig(props).readBlockBytes());
    }

    @Test
    public void readBlockBytesRejectsNonPositive() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.READ_BLOCK_BYTES_CONFIG, 0);
        assertThrows(ConfigException.class, () -> new ConverterConfig(props));
    }

    @Test
    public void readModeDefaultsToStream() {
        assertEquals(ReadMode.STREAM, new ConverterConfig(baseProps()).readMode());
    }

    @Test
    public void readModeHonorsCacheOverride() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.READ_MODE_CONFIG, ConverterConfig.READ_MODE_CACHE);
        assertEquals(ReadMode.CACHE, new ConverterConfig(props).readMode());
    }

    @Test
    public void readModeRejectsUnknownValue() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.READ_MODE_CONFIG, "mmap");
        assertThrows(ConfigException.class, () -> new ConverterConfig(props));
    }

    @Test
    public void readCacheDirDefaultsToTmpDir() {
        assertEquals(Paths.get(System.getProperty("java.io.tmpdir")),
                new ConverterConfig(baseProps()).readCacheDir());
    }

    @Test
    public void readCacheDirHonorsOverride() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.READ_CACHE_DIR_CONFIG, "/var/cache/segments");
        assertEquals(Paths.get("/var/cache/segments"), new ConverterConfig(props).readCacheDir());
    }
}
