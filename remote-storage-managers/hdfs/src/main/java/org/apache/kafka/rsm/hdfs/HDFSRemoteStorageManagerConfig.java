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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class HDFSRemoteStorageManagerConfig extends AbstractConfig {

    public static final String HDFS_BASE_DIR_PROP = "hdfs.base.dir";
    public static final String HDFS_BASE_DIR_DOC = "The HDFS directory in which the remote data is stored.";

    public static final String HDFS_REMOTE_READ_BYTES_PROP = "hdfs.remote.read.bytes";
    public static final String HDFS_REMOTE_READ_BYTES_DOC = "HDFS read buffer size in bytes.";
    public static final int DEFAULT_HDFS_REMOTE_READ_BYTES = 4 * 1024 * 1024;

    public static final String HDFS_REMOTE_READ_CACHE_BYTES_PROP = "hdfs.remote.read.cache.bytes";
    public static final String HDFS_REMOTE_READ_CACHE_BYTES_DOC = "Read cache size in bytes. " +
            "The maximum amount of remote data will be cached in broker's memory. " +
            "This value must be larger than " + HDFS_REMOTE_READ_BYTES_PROP;
    public static final long DEFAULT_HDFS_REMOTE_READ_CACHE_BYTES = 1024 * 1024 * 1024L;

    public static final String HDFS_USER_PROP = "hdfs.user";
    public static final String HDFS_KEYTAB_PATH_PROP = "hdfs.keytab.path";

    public static final String HDFS_USER_DOC = "The principal name to load from the keytab. " +
            "This property should be used with " + HDFS_KEYTAB_PATH_PROP;
    public static final String HDFS_KEYTAB_PATH_DOC = "The path to the keytab file. " +
            "This property should be used with " + HDFS_USER_PROP;

    public static final String HDFS_DEFAULT_FS_URI_PROP = "hdfs.default.file.system.uri";
    public static final String HDFS_DEFAULT_FS_URI_DOC = "The default File system URI for the HDFS cluster.";

    public static final String HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_PROP = "hdfs.remote.read.cache.buffer.pool.max.size";
    public static final String HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_DOC = "Maximum capacity of the buffer pool that manages ByteBuffer instances for remote data caching. Controls memory usage and buffer reuse efficiency.";
    public static final int DEFAULT_HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE = 256;
    
    public static final String HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP = "hdfs.read.error.backoff.wait.ms";
    public static final String HDFS_READ_ERROR_BACKOFF_WAIT_MS_DOC = "The amount of time to wait before sending the error response back to the client when it encounters remote read errors.";
    public static final long DEFAULT_HDFS_READ_ERROR_BACKOFF_WAIT_MS = 50;
     
    public static final String HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP = "hdfs.read.error.max.backoff.wait.ms";
    public static final String HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_DOC = "The maximum amount of time to wait before sending the error response back to the client when it encounters remote read errors.";
    public static final long DEFAULT_HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS = 500;

    public static final String HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP = "hdfs.dfs.client.hedged.read.threshold.millis";
    public static final String HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_DOC = "When hedged reads are enabled, " +
        "the number of milliseconds to wait before starting a second read against a different block replica";
    public static final int DEFAULT_HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS = 200;

    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP = "hdfs.dfs.client.read.threadpool.core_size";
    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_DOC = "The core size of the thread pool " +
        "dedicated for running hedged reads. Please note that a single instance of the threadpool is created per JVM/classloader " +
        "and shared by multiple instances of HDFSClient created within the same JVM/classloader.";
    public static final int DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE = 1;

    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP = "hdfs.dfs.client.read.threadpool.max_size";
    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_DOC = "The maximum size of the thread pool " +
        "dedicated for running hedged reads. Please note that a single instance of the threadpool is created per JVM/classloader " +
        "and shared by multiple instances of HDFSClient created within the same JVM/classloader.";
    public static final int DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE = 100;

    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP = "hdfs.dfs.client.read.threadpool.keep_alive_time";
    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_DOC = "The keep-alive time (in seconds) " +
        "for idle threads in the thread pool dedicated for running hedged reads.";
    public static final int DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS = 60;

    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP = "hdfs.dfs.client.read.threadpool.core-thread.timeout.allowed";
    public static final String HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_DOC = "Whether core threads " +
        "in the thread pool dedicated for running hedged reads are allowed to time out.";
    public static final boolean DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED = true;

    public static final String HDFS_OCI_BUCKETS_PROP = "hdfs.oci.buckets";
    public static final String HDFS_OCI_BUCKETS_DOC = "Comma separated list of OCI buckets";

    public static final String PREFETCH_LOCAL_BASE_DIR_PROP = "prefetch.local.base.dir";
    public static final String PREFETCH_LOCAL_BASE_DIR_DOC = "The local directory where remote log segments will be prefetched and stored temporarily. " +
        "This directory should have sufficient space to accommodate the cached segments.";
    public static final String DEFAULT_PREFETCH_LOCAL_BASE_DIR = "/tmp/remote-log-prefetch";

    public static final String PREFETCH_CACHE_MAX_SIZE_PROP = "prefetch.cache.max.size";
    public static final String PREFETCH_CACHE_MAX_SIZE_DOC = "Maximum size of the prefetch cache. " +
        "This limits the number of segments that can be cached in memory at any given time.";
    public static final int DEFAULT_PREFETCH_CACHE_MAX_SIZE = 1000;

    public static final String PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_PROP = "prefetch.cache.expire.after.access.time.minutes";
    public static final String PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_DOC = "The duration in minutes after which an entry in the prefetch cache expires if not accessed";
    public static final int DEFAULT_PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES = 10;

    public static final String PREFETCH_THREAD_POOL_CORE_SIZE_PROP = "prefetch.thread.pool.core.size";
    public static final String PREFETCH_THREAD_POOL_CORE_SIZE_DOC = "The core size of the thread pool used for prefetching segments. " +
        "This controls the number of concurrent prefetch operations.";
    public static final int DEFAULT_PREFETCH_THREAD_POOL_CORE_SIZE = 10;

    public static final String PREFETCH_THREAD_POOL_MAX_SIZE_PROP = "prefetch.thread.pool.max.size";
    public static final String PREFETCH_THREAD_POOL_MAX_SIZE_DOC = "The maximum size of the thread pool used for prefetching segments. " +
        "This limits the number of concurrent prefetch operations to avoid overwhelming the system.";
    public static final int DEFAULT_PREFETCH_THREAD_POOL_MAX_SIZE = 10;

    public static final String PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP = "prefetch.thread.pool.queue.capacity";
    public static final String PREFETCH_THREAD_POOL_QUEUE_CAPACITY_DOC = "The capacity of the queue used by the thread pool for prefetching segments. " +
        "This controls how many prefetch requests can be queued up before new requests are rejected.";
    public static final int DEFAULT_PREFETCH_THREAD_POOL_QUEUE_CAPACITY = 1000;

    public static final String OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP = "oci.prefetch.client.read.ahead.block.count";
    public static final String OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_DOC = "Number of blocks to read ahead by the prefetch client of OCI";
    public static final int DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT = 2;

    public static final String OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP = "oci.prefetch.client.read.ahead.block.size";
    public static final String OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_DOC = "Size in bytes of the blocks to read ahead by the prefetch client of OCI";
    public static final int DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE = 4194304;

    public static final String OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP = "oci.prefetch.client.read.ahead.num.threads";
    public static final String OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_DOC = "The number of threads to use for parallel GET operations when reading ahead by the prefetch client of OCI";
    public static final int DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS = 10;

    public static final String PREFETCH_MAX_BYTES_PER_SECOND_PROP = "prefetch.max.bytes.per.second";
    public static final String PREFETCH_MAX_BYTES_PER_SECOND_DOC = "The maximum number of bytes that can be prefetched from the remote storage to local storage per sec." +
        "This is a global limit for all the partitions that are being prefetched from remote storage to local storage. " +
        "The default value is Long.MAX_VALUE which means there is no limit on the number of bytes that can be fetched per second.";
    public static final Long DEFAULT_PREFETCH_MAX_BYTES_PER_SECOND = Long.MAX_VALUE;

    public static final String PREFETCH_QUOTA_WINDOW_NUM_PROP = "prefetch.quota.window.num";
    public static final String PREFETCH_QUOTA_WINDOW_NUM_DOC = "The number of samples to retain in memory for prefetch quota management. " +
        "The default value is 11, which means there are 10 whole windows + 1 current window.";
    public static final int DEFAULT_PREFETCH_QUOTA_WINDOW_NUM = 11;

    public static final String PREFETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP = "prefetch.quota.window.size.seconds";
    public static final String PREFETCH_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for prefetch quota management. " +
        "The default value is 1 second.";
    public static final int DEFAULT_PREFETCH_QUOTA_WINDOW_SIZE = 1;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
            .define(HDFS_BASE_DIR_PROP, STRING, HIGH, HDFS_BASE_DIR_DOC)
            .define(HDFS_USER_PROP, STRING, null, new ConfigDef.NonEmptyString(), MEDIUM, HDFS_USER_DOC)
            .define(HDFS_KEYTAB_PATH_PROP, STRING, null, new ConfigDef.NonEmptyString(), MEDIUM, HDFS_KEYTAB_PATH_DOC)
            .define(HDFS_REMOTE_READ_BYTES_PROP, INT, DEFAULT_HDFS_REMOTE_READ_BYTES, atLeast(1048576), MEDIUM, HDFS_REMOTE_READ_BYTES_DOC)
            .define(HDFS_REMOTE_READ_CACHE_BYTES_PROP, LONG, DEFAULT_HDFS_REMOTE_READ_CACHE_BYTES, atLeast(1048576), MEDIUM, HDFS_REMOTE_READ_CACHE_BYTES_DOC)
            .define(HDFS_DEFAULT_FS_URI_PROP, STRING, "", HIGH, HDFS_DEFAULT_FS_URI_DOC)
            .define(HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_PROP, INT, DEFAULT_HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE, atLeast(128), MEDIUM, HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_DOC)
            .define(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP, LONG, DEFAULT_HDFS_READ_ERROR_BACKOFF_WAIT_MS, atLeast(0), MEDIUM, HDFS_READ_ERROR_BACKOFF_WAIT_MS_DOC)
            .define(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP, LONG, DEFAULT_HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS, atLeast(0), MEDIUM, HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_DOC)
            .define(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP, LONG, DEFAULT_HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS, atLeast(1), MEDIUM, HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_DOC)
            .define(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP, INT, DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE, atLeast(1), MEDIUM, HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_DOC)
            .define(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP, INT, DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE, atLeast(1), MEDIUM, HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_DOC)
            .define(HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP,  INT, DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS, atLeast(1), MEDIUM, HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_DOC)
            .define(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP, BOOLEAN, DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED, MEDIUM, HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_DOC)
            .define(HDFS_OCI_BUCKETS_PROP, LIST, "", HIGH, HDFS_OCI_BUCKETS_DOC)
            .define(PREFETCH_LOCAL_BASE_DIR_PROP, ConfigDef.Type.STRING, DEFAULT_PREFETCH_LOCAL_BASE_DIR, ConfigDef.Importance.HIGH, PREFETCH_LOCAL_BASE_DIR_DOC)
            .define(PREFETCH_CACHE_MAX_SIZE_PROP, ConfigDef.Type.INT, DEFAULT_PREFETCH_CACHE_MAX_SIZE, atLeast(1), ConfigDef.Importance.HIGH, PREFETCH_CACHE_MAX_SIZE_DOC)
            .define(PREFETCH_THREAD_POOL_CORE_SIZE_PROP, ConfigDef.Type.INT, DEFAULT_PREFETCH_THREAD_POOL_CORE_SIZE, atLeast(1), ConfigDef.Importance.MEDIUM, PREFETCH_THREAD_POOL_CORE_SIZE_DOC)
            .define(PREFETCH_THREAD_POOL_MAX_SIZE_PROP, ConfigDef.Type.INT, DEFAULT_PREFETCH_THREAD_POOL_MAX_SIZE, atLeast(1), ConfigDef.Importance.MEDIUM, PREFETCH_THREAD_POOL_MAX_SIZE_DOC)
            .define(PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_PROP, ConfigDef.Type.INT, DEFAULT_PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES, atLeast(1), ConfigDef.Importance.MEDIUM, PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_DOC)
            .define(PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP, ConfigDef.Type.INT, DEFAULT_PREFETCH_THREAD_POOL_QUEUE_CAPACITY, atLeast(1), ConfigDef.Importance.MEDIUM, PREFETCH_THREAD_POOL_QUEUE_CAPACITY_DOC)
            .define(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, INT, DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT, atLeast(1), MEDIUM, OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_DOC)
            .define(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, INT, DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE, atLeast(1048576), MEDIUM, OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_DOC)
            .define(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, INT, DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS, atLeast(1), MEDIUM, OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_DOC)
            .define(PREFETCH_MAX_BYTES_PER_SECOND_PROP, LONG, DEFAULT_PREFETCH_MAX_BYTES_PER_SECOND, atLeast(1), MEDIUM, PREFETCH_MAX_BYTES_PER_SECOND_DOC)
            .define(PREFETCH_QUOTA_WINDOW_NUM_PROP, INT, DEFAULT_PREFETCH_QUOTA_WINDOW_NUM, atLeast(1), MEDIUM, PREFETCH_QUOTA_WINDOW_NUM_DOC)
            .define(PREFETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP, INT, DEFAULT_PREFETCH_QUOTA_WINDOW_SIZE, atLeast(1), MEDIUM, PREFETCH_QUOTA_WINDOW_SIZE_SECONDS_DOC);
    }

    public HDFSRemoteStorageManagerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

}
