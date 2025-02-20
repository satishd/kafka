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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PodUtilsTest {

    @Test
    public void testToPodAndRack() {
        assertEquals("pod1::rack1", PodUtils.toPodAndRack("pod1", "rack1"));
        assertEquals("rack1", PodUtils.toPodAndRack("", "rack1"));
        assertEquals("rack1", PodUtils.toPodAndRack(null, "rack1"));
        assertEquals("pod1::", PodUtils.toPodAndRack("pod1", ""));
        assertEquals("pod1::", PodUtils.toPodAndRack("pod1", null));
        assertEquals("", PodUtils.toPodAndRack("", ""));
        assertEquals("", PodUtils.toPodAndRack(null, ""));
        assertNull(PodUtils.toPodAndRack(null, null));
    }

    @Test
    public void testPodOf() {
        assertEquals("pod1", PodUtils.podOf("pod1::rack1"));
        assertEquals("", PodUtils.podOf("::rack1"));
        assertNull(PodUtils.podOf("rack1"));
    }

    @Test
    public void testRackOf() {
        assertEquals("rack1", PodUtils.rackOf("pod1::rack1"));
        assertEquals("rack1", PodUtils.rackOf("::rack1"));
        assertEquals("rack1", PodUtils.rackOf("rack1"));
        assertEquals("", PodUtils.rackOf("pod1::"));
        assertEquals("", PodUtils.rackOf("::"));
        assertEquals("", PodUtils.rackOf(""));
        assertNull(PodUtils.rackOf(null));

    }
}