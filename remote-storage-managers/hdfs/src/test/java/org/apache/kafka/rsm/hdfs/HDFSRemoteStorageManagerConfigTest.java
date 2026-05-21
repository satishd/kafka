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
package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HDFSRemoteStorageManagerConfigTest {

    @Test
    public void testPrefetchLocalBaseDir() {
        Map<String, String> props = defaultProps();

        // Verify default value
        HDFSRemoteStorageManagerConfig config = new HDFSRemoteStorageManagerConfig(props, false);
        assertEquals(HDFSRemoteStorageManagerConfig.DEFAULT_PREFETCH_LOCAL_BASE_DIR,
            config.getString(HDFSRemoteStorageManagerConfig.PREFETCH_LOCAL_BASE_DIR_PROP));

        // Verify Config exception is thrown for an empty string
        props.put(HDFSRemoteStorageManagerConfig.PREFETCH_LOCAL_BASE_DIR_PROP, "");
        assertThrows(ConfigException.class, () -> new HDFSRemoteStorageManagerConfig(props, false),
            "Invalid value  for configuration prefetch.local.base.dir: String must be non-empty");
    }

    @Test
    public void testDefaultPrefetchConfigs() {
        Map<String, String> props = defaultProps();
        HDFSRemoteStorageManagerConfig config = new HDFSRemoteStorageManagerConfig(props, false);
        assertFalse(config.getBoolean(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_ENABLE_PROP));
        assertEquals(HDFSRemoteStorageManagerConfig.DEFAULT_PREFETCH_CURRENT_SEGMENT_THRESHOLD_PERCENT,
            config.getInt(HDFSRemoteStorageManagerConfig.PREFETCH_CURRENT_SEGMENT_THRESHOLD_PERCENT_PROP));
        assertEquals(HDFSRemoteStorageManagerConfig.DEFAULT_PREFETCH_THREAD_POOL_QUEUE_CAPACITY,
            config.getInt(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP));
    }

    @Test
    public void testPrefetchCurrentSegmentThresholdPercent() {
        // valid configs
        List<Integer> validValues = Arrays.asList(-1, 0, 70);
        Map<String, String> props = defaultProps();
        HDFSRemoteStorageManagerConfig config;
        for (int expectedValue : validValues) {
            props.put(HDFSRemoteStorageManagerConfig.PREFETCH_CURRENT_SEGMENT_THRESHOLD_PERCENT_PROP, String.valueOf(expectedValue));
            config = new HDFSRemoteStorageManagerConfig(props, false);
            assertEquals(expectedValue, config.getInt(HDFSRemoteStorageManagerConfig.PREFETCH_CURRENT_SEGMENT_THRESHOLD_PERCENT_PROP));
        }

        props.put(HDFSRemoteStorageManagerConfig.PREFETCH_CURRENT_SEGMENT_THRESHOLD_PERCENT_PROP, "-2");
        assertThrows(ConfigException.class, () -> new HDFSRemoteStorageManagerConfig(props, false));

        props.put(HDFSRemoteStorageManagerConfig.PREFETCH_CURRENT_SEGMENT_THRESHOLD_PERCENT_PROP, "71");
        assertThrows(ConfigException.class, () -> new HDFSRemoteStorageManagerConfig(props, false));
    }

    private static Map<String, String> defaultProps() {
        Map<String, String> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        return props;
    }
}
