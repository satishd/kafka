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

import org.apache.kafka.rsm.hdfs.pool.ByteBufferWrapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple LRU cache of remote data.
 *
 * The cache is a hash map from (file path, offset) pairs to the corresponding data (byte[]).
 * When the cache is full (the total data size >= the maximum size), adding a new cache entry to the cache will replace
 * the least-recently-used entry.
 *
 * The data (byte[]) of each entry is assumed to have about the same size.
 */
class LRUCache {
    private long totalBytes;
    private LinkedHashMap<String, ByteBufferWrapper> cache;
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong loadCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);

    /**
     * Create a new LRU cache with the specified max size.
     * @param maxBytes The maximum bytes can be stored in this cache.
     */
    LRUCache(long maxBytes) {
        cache = new LinkedHashMap<String, ByteBufferWrapper>(1000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ByteBufferWrapper> eldest) {
                if (totalBytes >= maxBytes) {
                    totalBytes -= eldest.getValue().capacity();
                    evictionCount.incrementAndGet();
                    eldest.getValue().release();
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Adding a new entry into the cache, and replace the LRU entry if the cache is full.
     */
    synchronized void put(String path, long offset, ByteBufferWrapper data) {
        String key = path + ":" + offset;
        // Add the new entry to the cache. If an existing entry is replaced, release the previous entry.
        ByteBufferWrapper prev = cache.put(key, data.retain());
        if (prev != null) {
            totalBytes -= prev.capacity();
            prev.release();
        }
        totalBytes += data.capacity();
        loadCount.incrementAndGet();
    }

    /**
     * Retrieve the cached data with the specified (path, offset) pair.
     * Returns null if the required cache entry does not exist.
     */
    synchronized ByteBufferWrapper get(String path, long offset) {
        String key = path + ":" + offset;
        ByteBufferWrapper val = cache.get(key);
        if (val == null) {
            missCount.incrementAndGet();
            return null;
        } else {
            hitCount.incrementAndGet();
            return val.duplicate().retain();
        }
    }

    public CacheStats stats() {
        return new CacheStats(hitCount.get(), missCount.get(), loadCount.get(), evictionCount.get(), cache.size());
    }
}
