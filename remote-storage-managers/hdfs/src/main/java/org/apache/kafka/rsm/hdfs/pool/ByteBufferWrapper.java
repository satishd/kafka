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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBufferWrapper {
    private final AtomicInteger refCnt;
    private final ByteBuffer byteBuffer;
    private final ByteBufferPool pool;

    private ByteBufferWrapper(AtomicInteger refCnt, ByteBuffer byteBuffer, ByteBufferPool pool) {
        this.refCnt = refCnt;
        this.byteBuffer = byteBuffer;
        this.pool = pool;
    }

    public ByteBufferWrapper(ByteBuffer byteBuffer, ByteBufferPool pool) {
        this(new AtomicInteger(1), byteBuffer, pool);
    }

    public ByteBufferWrapper duplicate() {
        return new ByteBufferWrapper(refCnt, byteBuffer.duplicate(), pool);
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int refCnt() {
        return refCnt.get();
    }

    public int capacity() {
        return byteBuffer.capacity();
    }

    public ByteBufferWrapper retain() {
        refCnt.incrementAndGet();
        return this;
    }

    public void release() {
        if (refCnt.decrementAndGet() == 1) {
            pool.release(this);
        }
    }
}
