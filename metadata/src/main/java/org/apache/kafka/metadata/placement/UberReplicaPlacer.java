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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class UberReplicaPlacer implements ReplicaPlacer {
    private final ReplicaPlacer replicaPlacer;
    private final Supplier<Map<String, String>> clusterConfigSupplier;

    public UberReplicaPlacer(ReplicaPlacer replicaPlacer,
                             Supplier<Map<String, String>> clusterConfigSupplier) {
        this.replicaPlacer = replicaPlacer;
        this.clusterConfigSupplier = clusterConfigSupplier;
    }

    @Override
    public TopicAssignment place(PlacementSpec placement, ClusterDescriber cluster) throws InvalidReplicationFactorException {
        return replicaPlacer.place(placement, new ClusterDescriber() {
            @Override
            public Iterator<UsableBroker> usableBrokers() {
                return new UsableBrokerIterator(cluster.usableBrokers(), newReplicaExcludeList());
            }

            @Override
            public Uuid defaultDir(int brokerId) {
                return cluster.defaultDir(brokerId);
            }
        });
    }

    Set<Integer> newReplicaExcludeList() {
        Map<String, String> clusterConfigs = clusterConfigSupplier.get();
        String newReplicaExcludeListString = clusterConfigs.getOrDefault(ServerLogConfigs.NEW_REPLICA_EXCLUDE_LIST_CONFIG, "");
        return Arrays.stream(newReplicaExcludeListString.split(":"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(Integer::parseInt)
            .collect(Collectors.toSet());
    }

    private static final class UsableBrokerIterator extends AbstractIterator<UsableBroker> {

        private final Iterator<UsableBroker> original;
        private final Set<Integer> replicaExcludeList;

        private UsableBrokerIterator(Iterator<UsableBroker> original, Set<Integer> replicaExcludeList) {
            this.original = original;
            this.replicaExcludeList = replicaExcludeList;
        }

        @Override
        protected UsableBroker makeNext() {
            while (original.hasNext()) {
                UsableBroker usableBroker = original.next();
                if (replicaExcludeList.contains(usableBroker.id())) {
                    continue;
                }
                return usableBroker;
            }
            return this.allDone();
        }
    }
}
