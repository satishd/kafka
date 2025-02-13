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
package org.apache.kafka.admin;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class PodTypeTest {

    @Test
    void testFromPodName() {
        assertInstanceOf(Canary.class, PodType.fromPodName(Canary.POD_NAME));
        assertInstanceOf(Default.class, PodType.fromPodName(Default.POD_NAME));
        assertInstanceOf(Default.class, PodType.fromPodName(null));
        assertInstanceOf(Default.class, PodType.fromPodName(""));
        assertInstanceOf(Default.class, PodType.fromPodName("broker"));
    }

    @Test
    void testGetCanaryPartitions() {
        Canary canary = new Canary();
        Set<Integer> expected = new HashSet<>();

        assertEquals(0, canary.getPodPartitions(4, 0).size());
        assertEquals(0, canary.getPodPartitions(8, 0).size());
        expected.add(31);
        // Set(31) == {31}
        assertEquals(expected, canary.getPodPartitions(32, 0));
        assertEquals(expected, canary.getPodPartitions(24, 8));

        expected.add(63);
        // Set(31, 63) == {31, 63}
        assertEquals(expected, canary.getPodPartitions(64, 0));
        assertEquals(expected, canary.getPodPartitions(8, 64));

        expected.add(95);
        expected.add(127);
        // Set(31, 63, 95, 127) == {31, 63, 95, 127}
        assertEquals(expected, canary.getPodPartitions(128, 0));
    }

}