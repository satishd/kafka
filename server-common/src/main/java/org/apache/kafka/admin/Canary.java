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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Canary broker pod type.
 */
public class Canary implements PodType {
    public static final String POD_NAME = "canary-broker";
    private static final int CANARY_MIN_PARTITIONS = 32;
    private static final int CANARY_PARTITIONS_DIVIDER = 32;

    @Override
    public String getPodName() {
        return POD_NAME;
    }

    /**
     * Given topic partition count, returns eligible canary partitions.
     * A topic below minimum partition count threshold (32) is not eligible for canary partitions.
     * Roughly 3% of partitions are selected as canary partitions, and canary partition is the last partition in each range.
     */
    @Override
    public Set<Integer> getPodPartitions(int nPartitions, int startPartitionId) {
        int partitionCount = Math.max(0, startPartitionId) + nPartitions;
        if (partitionCount < CANARY_MIN_PARTITIONS) {
            return Collections.emptySet();
        } else {
            int canaryPartitionCount = partitionCount / CANARY_PARTITIONS_DIVIDER;
            Set<Integer> canaryPartitions = new HashSet<>();
            for (int i = 1; i <= canaryPartitionCount; i++) {
                if (i == 1) {
                    canaryPartitions.add(CANARY_MIN_PARTITIONS - 1);
                } else {
                    canaryPartitions.add(i * CANARY_PARTITIONS_DIVIDER - 1);
                }
            }
            return canaryPartitions;
        }
    }
}
