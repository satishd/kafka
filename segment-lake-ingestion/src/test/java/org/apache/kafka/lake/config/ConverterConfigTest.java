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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConverterConfigTest {

    private static Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConverterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConverterConfig.TOPICS_ALLOWLIST_CONFIG, "orders");
        return props;
    }

    @Test
    public void topicsAllowlistIsRequired() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConverterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        assertThrows(ConfigException.class, () -> new ConverterConfig(props));
    }

    @Test
    public void topicsAllowlistRejectsEmptyValue() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.TOPICS_ALLOWLIST_CONFIG, "");
        assertThrows(ConfigException.class, () -> new ConverterConfig(props));
    }

    @Test
    public void topicsAllowlistRejectsBlankTopic() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.TOPICS_ALLOWLIST_CONFIG, "orders, ,payments");
        assertThrows(ConfigException.class, () -> new ConverterConfig(props));
    }

    @Test
    public void topicsAllowlistIsParsed() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.TOPICS_ALLOWLIST_CONFIG, "orders,payments");
        assertEquals(Arrays.asList("orders", "payments"),
                new ConverterConfig(props).topicsAllowlist());
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

    @Test
    public void writeStageDefaults() {
        ConverterConfig config = new ConverterConfig(baseProps());
        assertEquals(8, config.writeQueueCapacity());
        assertEquals(3, config.writeMaxRetries());
        assertEquals(1000L, config.writeRetryBackoffMs());
        assertEquals(5000L, config.offsetCommitIntervalMs());
    }

    @Test
    public void writeStageHonorsOverrides() {
        Map<String, Object> props = baseProps();
        props.put(ConverterConfig.WRITE_QUEUE_CAPACITY_CONFIG, 32);
        props.put(ConverterConfig.WRITE_MAX_RETRIES_CONFIG, 0);
        props.put(ConverterConfig.WRITE_RETRY_BACKOFF_MS_CONFIG, 250);
        props.put(ConverterConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, 200);
        ConverterConfig config = new ConverterConfig(props);
        assertEquals(32, config.writeQueueCapacity());
        assertEquals(0, config.writeMaxRetries());
        assertEquals(250L, config.writeRetryBackoffMs());
        assertEquals(200L, config.offsetCommitIntervalMs());
    }

    @Test
    public void writeStageRejectsInvalidValues() {
        Map<String, Object> zeroQueue = baseProps();
        zeroQueue.put(ConverterConfig.WRITE_QUEUE_CAPACITY_CONFIG, 0);
        assertThrows(ConfigException.class, () -> new ConverterConfig(zeroQueue));

        Map<String, Object> negativeRetries = baseProps();
        negativeRetries.put(ConverterConfig.WRITE_MAX_RETRIES_CONFIG, -1);
        assertThrows(ConfigException.class, () -> new ConverterConfig(negativeRetries));
    }

    @Test
    public void consumerDisablesAutoCommit() {
        assertEquals("false",
                new ConverterConfig(baseProps()).consumerProperties().getProperty("enable.auto.commit"));
    }
}
