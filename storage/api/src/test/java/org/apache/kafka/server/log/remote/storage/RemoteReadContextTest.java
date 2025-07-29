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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.server.common.OffsetAndEpoch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoteReadContextTest {

    @Test
    public void testConstructorAndGetters() {
        // Test with null nextSegmentOffsetAndEpoch
        RemoteReadContext context1 = new RemoteReadContext(true, false, null, false);
        assertTrue(context1.isBlockPrefetchEnabled());
        assertFalse(context1.isHedgedReadsEnabled());
        assertFalse(context1.isSegmentPrefetchEnabled());
        assertNull(context1.getNextSegmentOffsetAndEpoch());

        // Test with non-null nextSegmentOffsetAndEpoch
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(100L, 5);
        RemoteReadContext context2 = new RemoteReadContext(false, true, offsetAndEpoch, true);
        assertFalse(context2.isBlockPrefetchEnabled());
        assertTrue(context2.isHedgedReadsEnabled());
        assertTrue(context2.isSegmentPrefetchEnabled());
        assertEquals(offsetAndEpoch, context2.getNextSegmentOffsetAndEpoch());
        assertEquals(100L, context2.getNextSegmentOffsetAndEpoch().offset());
        assertEquals(5, context2.getNextSegmentOffsetAndEpoch().leaderEpoch());
    }

    @Test
    public void testEqualsAndHashCode() {
        // Create test objects with different parameter combinations
        OffsetAndEpoch offsetAndEpoch1 = new OffsetAndEpoch(100L, 5);
        OffsetAndEpoch offsetAndEpoch2 = new OffsetAndEpoch(200L, 10);
        
        // Reference object
        RemoteReadContext context = new RemoteReadContext(true, false, offsetAndEpoch1, false);
        
        // Equal objects
        RemoteReadContext equalContext1 = new RemoteReadContext(true, false, offsetAndEpoch1, false);
        RemoteReadContext equalContext2 = new RemoteReadContext(true, false, offsetAndEpoch1, false);
        
        // Different objects - varying each parameter
        RemoteReadContext differentPrefetchEnabled = new RemoteReadContext(false, false, offsetAndEpoch1, false);
        RemoteReadContext differentHedgedReadsEnabled = new RemoteReadContext(true, true, offsetAndEpoch1, false);
        RemoteReadContext differentOffsetAndEpoch = new RemoteReadContext(true, false, offsetAndEpoch2, false);
        RemoteReadContext differentNullOffsetAndEpoch = new RemoteReadContext(true, false, null, false);
        RemoteReadContext differentRemoteStoragePrefetchEnabled = new RemoteReadContext(true, false, offsetAndEpoch1, true);
        
        // Test reflexivity: x.equals(x) should be true
        assertTrue(context.equals(context), "An object should equal itself");
        
        // Test symmetry: if x.equals(y) then y.equals(x)
        assertTrue(context.equals(equalContext1), "Equal objects should be symmetric");
        assertTrue(equalContext1.equals(context), "Equal objects should be symmetric");
        
        // Test transitivity: if x.equals(y) and y.equals(z) then x.equals(z)
        assertTrue(context.equals(equalContext1), "First equality for transitivity");
        assertTrue(equalContext1.equals(equalContext2), "Second equality for transitivity");
        assertTrue(context.equals(equalContext2), "Transitive equality should hold");
        
        // Test consistency: multiple invocations should return same result
        boolean firstResult = context.equals(equalContext1);
        boolean secondResult = context.equals(equalContext1);
        assertEquals(firstResult, secondResult, "Multiple equality checks should be consistent");
        
        // Test non-equality with null
        assertFalse(context.equals(null), "Object should not equal null");
        
        // Test non-equality with different type
        assertFalse(context.equals("string"), "Object should not equal different type");
        
        // Test inequality with different field values
        assertFalse(context.equals(differentPrefetchEnabled), "Objects with different prefetchEnabled should not be equal");
        assertFalse(context.equals(differentHedgedReadsEnabled), "Objects with different hedgedReadsEnabled should not be equal");
        assertFalse(context.equals(differentOffsetAndEpoch), "Objects with different offsetAndEpoch should not be equal");
        assertFalse(context.equals(differentNullOffsetAndEpoch), "Objects with null vs non-null offsetAndEpoch should not be equal");
        assertFalse(context.equals(differentRemoteStoragePrefetchEnabled), "Objects with different remoteStoragePrefetchEnabled should not be equal");
        
        // Test hashCode contract
        // Equal objects must have equal hash codes
        assertEquals(context.hashCode(), equalContext1.hashCode(), "Equal objects should have equal hash codes");
        assertEquals(equalContext1.hashCode(), equalContext2.hashCode(), "Equal objects should have equal hash codes");
        assertEquals(context.hashCode(), equalContext2.hashCode(), "Equal objects should have equal hash codes");
        
        // Unequal objects should have different hash codes (not guaranteed but good practice)
        assertNotEquals(context.hashCode(), differentPrefetchEnabled.hashCode(), "Different prefetchEnabled should result in different hash codes");
        assertNotEquals(context.hashCode(), differentHedgedReadsEnabled.hashCode(), "Different hedgedReadsEnabled should result in different hash codes");
        assertNotEquals(context.hashCode(), differentOffsetAndEpoch.hashCode(), "Different offsetAndEpoch should result in different hash codes");
        assertNotEquals(context.hashCode(), differentNullOffsetAndEpoch.hashCode(), "Null vs non-null offsetAndEpoch should result in different hash codes");
        assertNotEquals(context.hashCode(), differentRemoteStoragePrefetchEnabled.hashCode(), "Different remoteStoragePrefetchEnabled should result in different hash codes");
        
        // Test consistency of hashCode
        int firstHashCode = context.hashCode();
        int secondHashCode = context.hashCode();
        assertEquals(firstHashCode, secondHashCode, "Multiple hashCode invocations should be consistent");
    }

    @Test
    public void testToString() {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(100L, 5);
        RemoteReadContext context = new RemoteReadContext(true, false, offsetAndEpoch, true);
        
        String toString = context.toString();
        assertTrue(toString.contains("prefetchEnabled=true"));
        assertTrue(toString.contains("hedgedReadsEnabled=false"));
        assertTrue(toString.contains("nextSegmentOffsetAndEpoch=" + offsetAndEpoch));
        assertTrue(toString.contains("segmentPrefetchEnabled=true"));
    }
}