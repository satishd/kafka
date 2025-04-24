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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ByteBufferWrapperTest {

    @Test
    public void testLifeCycle() {
        ByteBufferPool pool = new ByteBufferPoolImpl(1024, 10);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBufferWrapper wrapper = new ByteBufferWrapper(buffer, pool);

        // On creation, refCnt should be 1
        assertEquals(1, wrapper.refCnt());
        // Verify the capacity
        assertEquals(1024, wrapper.capacity());

        // Retain the buffer
        wrapper.retain();
        assertEquals(2, wrapper.refCnt());

        // Call duplicate and verify the duplicate wrapper
        ByteBufferWrapper duplicate = wrapper.duplicate();
        assertEquals(2, wrapper.refCnt());
        assertEquals(2, duplicate.refCnt());
        assertTrue(isDuplicate(wrapper.getByteBuffer(), duplicate.getByteBuffer()));

        // Calling retain on duplicate should increase the ref count for original and duplicate
        duplicate.retain();
        assertEquals(3, duplicate.refCnt());
        assertEquals(3, wrapper.refCnt());

        // Release the orignal wrapper, it wont trigger release to the pool, since ref Count is > 1
        wrapper.release();
        assertEquals(2, wrapper.refCnt());
        assertEquals(2, duplicate.refCnt());
        assertEquals(0, pool.releaseCount());

        // Release the duplicate, it should trigger release to the pool
        duplicate.release();
        assertEquals(1, wrapper.refCnt());
        assertEquals(1, duplicate.refCnt());
        assertEquals(1, pool.releaseCount());
    }

    private boolean isDuplicate(ByteBuffer buffer1, ByteBuffer buffer2) {
        return buffer1 != buffer2
                && buffer1.capacity() == buffer2.capacity()
                && buffer1.hasArray() && buffer2.hasArray()
                && buffer1.array() == buffer2.array()
                && buffer1.arrayOffset() == buffer2.arrayOffset();
    }
}
