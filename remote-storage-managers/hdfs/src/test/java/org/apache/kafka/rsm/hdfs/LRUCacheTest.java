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

import org.apache.kafka.rsm.hdfs.pool.ByteBufferPool;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferPoolImpl;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferWrapper;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LRUCacheTest {

    @Test
    public void testLRUCache() {
        LRUCache cache = new LRUCache(1000);
        ByteBufferPool pool = new ByteBufferPoolImpl(10, 100);
        for (int i = 0; i < 100; i++)
            cache.put("a", i * 10, bufferWrapper(String.format("a%09d", i).getBytes(StandardCharsets.UTF_8), pool));

        // access for key "a:0" will make it the most recently used entry and "a:10" the least recently used entry
        assertEquals(String.format("a%09d", 0), new String(cache.get("a", 0).getByteBuffer().array(), StandardCharsets.UTF_8));
        // put a new entry will evict the least recently used entry "a:10" and make "a:20" the least recently used entry
        cache.put("b",  123, bufferWrapper(String.format("b%09d", 0).getBytes(StandardCharsets.UTF_8), pool));
        // access for key "a:10" will return null now
        assertNull(cache.get("a", 10));
        // access for key "b:123" does not change the order of the entries
        assertEquals(String.format("b%09d", 0), new String(cache.get("b", 123).getByteBuffer().array(), StandardCharsets.UTF_8));
        // putting a new entry will evict the least recently used entry "a:20" and make "a:30" the least recently used entry
        cache.put("b",  456, bufferWrapper(String.format("b%09d", 1).getBytes(StandardCharsets.UTF_8), pool));
        // access for key "a:20" will return null now
        assertNull(cache.get("a", 20));
        // verify the entry against "b:456"
        assertEquals(String.format("b%09d", 1), new String(cache.get("b", 456).getByteBuffer().array(), StandardCharsets.UTF_8));
        // Replace the entry for "a:30" with a new entry, but fewer bytes than before (previous 10, new 4)
        cache.put("a", 30, bufferWrapper("test".getBytes(), pool));
        // Putting a new entry will not evict any entry, because the total size of the cache reduced when we added the new entry of 4 bytes
        cache.put("b",  3333, bufferWrapper(String.format("b%09d", 2).getBytes(StandardCharsets.UTF_8), pool));
        // Verify that the entry for "a:40" is still there
        assertNotNull(cache.get("a", 40));
        // Verify the entry for "a:30" exists with the new value
        assertEquals("test", new String(cache.get("a", 30).getByteBuffer().array()));

        // Verify cache stats
        assertEquals(5, cache.stats().getHitCount());
        assertEquals(2, cache.stats().getMissCount());
        assertEquals(5.0 / 7, cache.stats().getHitRate());
        assertEquals(2.0 / 7, cache.stats().getMissRate());
        assertEquals(7, cache.stats().getRequestCount());
        assertEquals(104, cache.stats().getLoadCount());
        assertEquals(2, cache.stats().getEvictionCount());
        assertEquals(101, cache.stats().getSize());
    }

    @Test
    public void testCacheEntryLifeCycle() {
        LRUCache cache = new LRUCache(10);
        byte[] data = "0123456789".getBytes();
        ByteBufferPoolImpl pool = new ByteBufferPoolImpl(10, 100);

        ByteBuffer val1Buffer = ByteBuffer.allocate(10).put(data);
        ByteBufferWrapper val1 = new ByteBufferWrapper(val1Buffer, pool);
        // Ref count is 1 when just allocated
        assertEquals(1, val1.refCnt());

        // Putting it into the cache will increase the ref count, because cache should retain one reference
        cache.put("a", 0, val1);
        assertEquals(2, val1.refCnt());

        // Getting it from the cache will increase the ref count, because one reference is returned to the caller
        ByteBufferWrapper val2 = cache.get("a", 0);
        // Both val1 and val2 should have ref count of 3, since val2 is derived from val1
        assertEquals(3, val1.refCnt());
        assertEquals(3, val2.refCnt());

        // Releasing val2 should decrease the ref count
        val2.release();
        assertEquals(2, val1.refCnt());
        // Releasing val1 should decrease the ref count
        val1.release();
        assertEquals(1, val1.refCnt());

        // Replacing the cache entry should decrease the ref count of the replaced entry
        ByteBuffer replacementBuffer = ByteBuffer.allocate(10).put(data);
        ByteBufferWrapper replacement = new ByteBufferWrapper(replacementBuffer, pool);
        cache.put("a", 0, replacement);
        assertEquals(0, val1.refCnt());
        // The ref count of the replacement should be 2, because the cache retains one reference
        assertEquals(2, replacement.refCnt());

        // Adding a new entry into the cache will evict the previous entry because of size limit.
        ByteBuffer newValBuffer = ByteBuffer.allocate(10).put(data);
        ByteBufferWrapper newVal = new ByteBufferWrapper(newValBuffer, pool);
        cache.put("a", 1, newVal);
        assertEquals(1, cache.stats().getSize());
        assertNull(cache.get("a", 0));
        // On eviction the ref count should be decreased
        assertEquals(1, replacement.refCnt());
    }

    private ByteBufferWrapper bufferWrapper(byte[] bytes, ByteBufferPool pool) {
        return new ByteBufferWrapper(ByteBuffer.wrap(bytes), pool);
    }
}
