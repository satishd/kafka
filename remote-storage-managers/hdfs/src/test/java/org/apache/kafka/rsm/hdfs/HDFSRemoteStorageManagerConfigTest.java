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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HDFSRemoteStorageManagerConfigTest {

    @Test
    public void testDefaultHedgedReadsProps() {
        Map<String, String> props = defaultProps();
        HDFSRemoteStorageManagerConfig config = new HDFSRemoteStorageManagerConfig(props, false);

        assertEquals(200L, config.getLong(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP));
        assertEquals(1, config.getInt(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP));
        assertEquals(100, config.getInt(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP));
        assertEquals(60, config.getInt(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP));
        assertTrue(config.getBoolean(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP));
    }

    @Test
    public void testHedgedReadsProps() {
        Map<String, String> props = defaultProps();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP, "100");
        props.put(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP, "2");
        props.put(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP, "10");
        props.put(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP, "30");
        props.put(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP, "false");
        HDFSRemoteStorageManagerConfig config = new HDFSRemoteStorageManagerConfig(props, false);

        assertEquals(100L, config.getLong(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP));
        assertEquals(2, config.getInt(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP));
        assertEquals(10, config.getInt(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP));
        assertEquals(30, config.getInt(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP));
        assertFalse(config.getBoolean(HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP));
    }

    private static Map<String, String> defaultProps() {
        Map<String, String> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        return props;
    }
}
