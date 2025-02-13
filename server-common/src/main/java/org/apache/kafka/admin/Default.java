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

import java.util.HashSet;
import java.util.Set;

/**
 * Default broker pod type
 */
public class Default implements PodType {
    // default podName does not matter as it's the fallback type
    public static final String POD_NAME = "default";

    @Override
    public String getPodName() {
        return POD_NAME;
    }

    /**
     *  Default implementation returns all partitions.
     */
    @Override
    public Set<Integer> getPodPartitions(int nPartitions, int startPartitionId) {
        int currentPartitionId = Math.max(0, startPartitionId);
        Set<Integer> partitions = new HashSet<>();
        for (int i = currentPartitionId; i < currentPartitionId + nPartitions; i++) {
            partitions.add(i);
        }
        return partitions;
    }
}
