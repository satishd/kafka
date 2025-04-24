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

package org.apache.kafka.rsm.hdfs.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ByteBufferPoolImpl implements ByteBufferPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferPoolImpl.class);

    private final ConcurrentLinkedQueue<ByteBuffer> buffers = new ConcurrentLinkedQueue<>();
    private final int bufferSize;
    private final int maxPoolSize;
    private final AtomicLong allocationCount = new AtomicLong(0);
    private final AtomicLong releaseCount = new AtomicLong(0);
    private final AtomicLong reuseCount = new AtomicLong(0);
    private final AtomicLong discardedCount = new AtomicLong(0);
    private final AtomicLong recycledCount = new AtomicLong(0);
    private final AtomicInteger poolSize = new AtomicInteger(0);

    public ByteBufferPoolImpl(int bufferSize, int maxPoolSize) {
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
    }

    public ByteBufferWrapper acquire() {
        ByteBuffer buf = buffers.poll();

        if (buf != null) {
            poolSize.decrementAndGet();
            long reuseCount = this.reuseCount.incrementAndGet();
            LOGGER.trace("Reused buffer of size {}. Total reuse count: {}", bufferSize, reuseCount);
        } else {
            buf = ByteBuffer.allocate(bufferSize);
            long allocationCount = this.allocationCount.incrementAndGet();
            LOGGER.trace("Allocated new buffer of size {}. Total allocation count: {}", bufferSize, allocationCount);
        }

        return new ByteBufferWrapper(buf, this);
    }

    public void release(ByteBufferWrapper wrapper) {
        releaseCount.incrementAndGet();
        if (wrapper.refCnt() != 1) {
            LOGGER.warn("Buffer reference count is not 1: {}", wrapper.refCnt());
            return;
        }

        if (wrapper.capacity() != bufferSize) {
            LOGGER.warn("Buffer capacity is not {}: {}", bufferSize, wrapper.capacity());
            return;
        }

        ByteBuffer byteBuffer = wrapper.getByteBuffer();
        byteBuffer.clear();

        boolean recycled = false;
        if (poolSize.get() < maxPoolSize) {
            if (poolSize.incrementAndGet() <= maxPoolSize) {
                buffers.offer(byteBuffer);
                long recycledCount = this.recycledCount.incrementAndGet();
                recycled = true;
                LOGGER.trace("Recycled buffer of size {}. Total recycled count: {}", bufferSize, recycledCount);
            } else {
                // We incremented over the limit, so decrement back
                poolSize.decrementAndGet();
            }
        }

        if (!recycled) {
            long discardCount = discardedCount.incrementAndGet();
            LOGGER.trace("Discarded buffer of size {}, since pool size has reached max size. Total discard count: {}", bufferSize, discardCount);
        }
    }

    public long allocCount() {
        return allocationCount.get();
    }

    public long releaseCount() {
        return releaseCount.get();
    }

    public long reuseCount() {
        return reuseCount.get();
    }

    public int poolSize() {
        return poolSize.get();
    }

    public long discardCount() {
        return discardedCount.get();
    }

    public long recycleCount() {
        return recycledCount.get();
    }
}
