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
        RemoteReadContext context1 = new RemoteReadContext(true, false, null);
        assertTrue(context1.isPrefetchEnabled());
        assertFalse(context1.isHedgedReadsEnabled());
        assertNull(context1.getNextSegmentOffsetAndEpoch());

        // Test with non-null nextSegmentOffsetAndEpoch
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(100L, 5);
        RemoteReadContext context2 = new RemoteReadContext(false, true, offsetAndEpoch);
        assertFalse(context2.isPrefetchEnabled());
        assertTrue(context2.isHedgedReadsEnabled());
        assertEquals(offsetAndEpoch, context2.getNextSegmentOffsetAndEpoch());
        assertEquals(100L, context2.getNextSegmentOffsetAndEpoch().offset());
        assertEquals(5, context2.getNextSegmentOffsetAndEpoch().leaderEpoch());
    }

    @Test
    public void testEqualsAndHashCode() {
        OffsetAndEpoch offsetAndEpoch1 = new OffsetAndEpoch(100L, 5);
        OffsetAndEpoch offsetAndEpoch2 = new OffsetAndEpoch(200L, 10);

        RemoteReadContext context1 = new RemoteReadContext(true, false, offsetAndEpoch1);
        RemoteReadContext context2 = new RemoteReadContext(true, false, offsetAndEpoch1);
        RemoteReadContext context3 = new RemoteReadContext(false, false, offsetAndEpoch1);
        RemoteReadContext context4 = new RemoteReadContext(true, true, offsetAndEpoch1);
        RemoteReadContext context5 = new RemoteReadContext(true, false, offsetAndEpoch2);
        RemoteReadContext context6 = new RemoteReadContext(true, false, null);

        // Test equals
        assertEquals(context1, context2);
        assertNotEquals(context1, context3);
        assertNotEquals(context1, context4);
        assertNotEquals(context1, context5);
        assertNotEquals(context1, context6);

        // Test hashCode
        assertEquals(context1.hashCode(), context2.hashCode());
        assertNotEquals(context1.hashCode(), context3.hashCode());
        assertNotEquals(context1.hashCode(), context4.hashCode());
        assertNotEquals(context1.hashCode(), context5.hashCode());
        assertNotEquals(context1.hashCode(), context6.hashCode());
    }

    @Test
    public void testToString() {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(100L, 5);
        RemoteReadContext context = new RemoteReadContext(true, false, offsetAndEpoch);
        
        String toString = context.toString();
        assertTrue(toString.contains("prefetchEnabled=true"));
        assertTrue(toString.contains("hedgedReadsEnabled=false"));
        assertTrue(toString.contains("nextSegmentOffsetAndEpoch=" + offsetAndEpoch.toString()));
    }
}