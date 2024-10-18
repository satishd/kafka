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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CacheStatsTest {

    @Test
    public void testStats() {
        CacheStats emptyStats = new CacheStats(0, 0, 0, 0, 0);
        assertEquals(0, emptyStats.getHitCount());
        assertEquals(0, emptyStats.getMissCount());
        assertEquals(0, emptyStats.getRequestCount());
        assertEquals(0.0, emptyStats.getHitRate());
        assertEquals(0.0, emptyStats.getMissRate());
        assertEquals(0, emptyStats.getLoadCount());
        assertEquals(0, emptyStats.getEvictionCount());

        CacheStats nonEmptyStats = new CacheStats(8, 2, 20, 5, 10);
        assertEquals(8, nonEmptyStats.getHitCount());
        assertEquals(2, nonEmptyStats.getMissCount());
        assertEquals(10, nonEmptyStats.getRequestCount());
        assertEquals(0.8, nonEmptyStats.getHitRate());
        assertEquals(0.2, nonEmptyStats.getMissRate());
        assertEquals(20, nonEmptyStats.getLoadCount());
        assertEquals(5, nonEmptyStats.getEvictionCount());
        assertEquals(10, nonEmptyStats.getSize());
    }
}
