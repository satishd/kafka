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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ByteBufferPoolImplTest {

    @Test
    public void testAcquire() {
        // Create a ByteBufferPoolImpl instance
        ByteBufferPoolImpl pool = new ByteBufferPoolImpl(1024, 10);

        // Acquire a buffer
        ByteBufferWrapper bufferWrapper = pool.acquire();

        // Verify the buffer properties
        verifyAcquiredBuffer(bufferWrapper);

        TestBufferPoolStats stats = new TestBufferPoolStats().incrementAllocCount();
        verifyStats(stats, pool);

        // Retain the buffer
        bufferWrapper.retain();
        assertEquals(2, bufferWrapper.refCnt());

        // Release the buffer back to the pool
        bufferWrapper.release();
        assertEquals(1, bufferWrapper.refCnt());

        stats.incrementReleaseCount().incrementRecycledCount().incrementPoolSize();
        verifyStats(stats, pool);

        // Acquire again, this time no allocation should happen
        ByteBufferWrapper newBufferWrapper = pool.acquire();
        verifyAcquiredBuffer(newBufferWrapper);

        // Verify the stats on reuse
        stats.incrementReuseCount().decrementPoolSize();
        verifyStats(stats, pool);

        // retain and release the buffer
        newBufferWrapper.retain();
        assertEquals(2, newBufferWrapper.refCnt());
        newBufferWrapper.release();

        // verify the new stats after release
        stats.incrementReleaseCount().incrementRecycledCount().incrementPoolSize();
        verifyStats(stats, pool);
    }

    @Test
    public void testReleaseWithInvalidRefCount() {
        // Create a ByteBufferPoolImpl instance
        ByteBufferPoolImpl pool = new ByteBufferPoolImpl(1024, 10);

        // Acquire a buffer
        ByteBufferWrapper bufferWrapper = pool.acquire();
        assertEquals(1, bufferWrapper.refCnt());

        TestBufferPoolStats stats = new TestBufferPoolStats().incrementAllocCount();
        verifyStats(stats, pool);

        // Verify the buffer properties
        verifyAcquiredBuffer(bufferWrapper);
        bufferWrapper.retain();
        assertEquals(2, bufferWrapper.refCnt());

        // Release the buffer back to the pool, but this should not go back to the pool
        // (explicitly calling release on the pool, since the ByteBufferWrapper method wont call since refCnt is 1)
        pool.release(bufferWrapper);

        // Verify buffer not added to pool
        stats.incrementReleaseCount();
        verifyStats(stats, pool);
    }

    @Test
    public void testReleaseWithInvalidByteBuffer() {
        // Create a ByteBufferPoolImpl instance
        ByteBufferPoolImpl pool = new ByteBufferPoolImpl(1024, 10);

        // Acquire a buffer with a different size
        ByteBufferWrapper bufferWrapper = new ByteBufferWrapper(ByteBuffer.allocate(2048), pool);
        bufferWrapper.retain();
        assertEquals(2, bufferWrapper.refCnt());

        // All stats should be zeros, since the buffer is not yet acquired from the pool
        TestBufferPoolStats stats = new TestBufferPoolStats();
        verifyStats(stats, pool);

        bufferWrapper.release();

        // Verify buffer was not recycled
        stats.incrementReleaseCount();
        verifyStats(stats, pool);
    }

    @Test
    public void testReleaseWithPoolFull() {
        // Create a ByteBufferPoolImpl instance with a small pool size
        ByteBufferPoolImpl pool = new ByteBufferPoolImpl(1024, 1);
        TestBufferPoolStats stats = new TestBufferPoolStats();

        // Acquire two buffers
        ByteBufferWrapper bufferWrapper1 = pool.acquire();
        ByteBufferWrapper bufferWrapper2 = pool.acquire();
        stats.incrementAllocCount().incrementAllocCount();
        verifyStats(stats, pool);

        // Retain the buffers
        bufferWrapper1.retain();
        bufferWrapper2.retain();

        // Release one buffer so pool becomes full
        bufferWrapper1.release();
        stats.incrementReleaseCount().incrementRecycledCount().incrementPoolSize();
        verifyStats(stats, pool);

        // Release the second buffer, which should be discarded since the pool is full
        bufferWrapper2.release();
        stats.incrementReleaseCount().incrementDiscardedCount();
        verifyStats(stats, pool);
    }

    private static void verifyAcquiredBuffer(ByteBufferWrapper buffer) {
        assertNotNull(buffer);
        assertEquals(1024, buffer.capacity());
        assertEquals(1, buffer.refCnt());
    }

    private static void verifyStats(TestBufferPoolStats stats, ByteBufferPool pool) {
        assertEquals(stats.allocCount, pool.allocCount());
        assertEquals(stats.releaseCount, pool.releaseCount());
        assertEquals(stats.reuseCount, pool.reuseCount());
        assertEquals(stats.discardedCount, pool.discardCount());
        assertEquals(stats.recycledCount, pool.recycleCount());
        assertEquals(stats.poolSize, pool.poolSize());
    }

    private static class TestBufferPoolStats {
        private int allocCount;
        private int releaseCount;
        private int reuseCount;
        private int discardedCount;
        private int recycledCount;
        private int poolSize;

        public TestBufferPoolStats incrementAllocCount() {
            allocCount++;
            return this;
        }

        public TestBufferPoolStats incrementReleaseCount() {
            releaseCount++;
            return this;
        }

        public TestBufferPoolStats incrementReuseCount() {
            reuseCount++;
            return this;
        }

        public TestBufferPoolStats incrementDiscardedCount() {
            discardedCount++;
            return this;
        }

        public TestBufferPoolStats incrementRecycledCount() {
            recycledCount++;
            return this;
        }

        public TestBufferPoolStats incrementPoolSize() {
            poolSize++;
            return this;
        }

        public TestBufferPoolStats decrementPoolSize() {
            poolSize--;
            return this;
        }
    }
}
