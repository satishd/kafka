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

public class CacheStats {
    private final long hitCount;
    private final long missCount;
    private final long loadCount;
    private final long evictionCount;
    private final int size;

    public CacheStats(long hitCount, long missCount, long loadCount, long evictionCount, int size) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.loadCount = loadCount;
        this.evictionCount = evictionCount;
        this.size = size;
    }

    public long getRequestCount() {
        return hitCount + missCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public double getHitRate() {
        if (getRequestCount() == 0) {
            return 0.0;
        } else {
            return (getHitCount() * 1.0) / getRequestCount();
        }
    }

    public long getMissCount() {
        return missCount;
    }

    public double getMissRate() {
        if (getRequestCount() == 0) {
            return 0.0;
        } else {
            return (getMissCount() * 1.0) / getRequestCount();
        }
    }

    public long getLoadCount() {
        return loadCount;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "{" +
            "\"requestCount\": " + getRequestCount() + "," +
            "\"hitCount\": " + getHitCount() + "," +
            "\"hitRate\": " + getHitRate() + "," +
            "\"missCount\": " + getMissCount() + "," +
            "\"missRate\": " + getMissRate() + "," +
            "\"loadCount\": " + getLoadCount() + "," +
            "\"evictionCount\": " + getEvictionCount() + "," +
            "\"size\": " + getSize() +
            "}";
    }
}
