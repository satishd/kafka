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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ControllerMetadataUrpMetricsTest {

    private static MetricName urpsMetricNameForBroker(int brokerId) {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("broker_id", Integer.toString(brokerId));
        return KafkaYammerMetrics.getMetricName(
            "kafka.controller",
            "KafkaController",
            "UrpsCausedByBroker",
            tags
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUrpGaugeLifecycleAndUpdate() {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            // Initially, no URP metrics exist
            assertNull(registry.allMetrics().get(urpsMetricNameForBroker(1)));

            // Create/update via bulk update
            java.util.Map<Integer, Integer> counts = new java.util.HashMap<>();
            counts.put(1, 5);
            counts.put(2, 3);
            metrics.updateUrpsByBroker(counts);

            Gauge<Integer> g1 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(1));
            Gauge<Integer> g2 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(2));
            assertNotNull(g1);
            assertNotNull(g2);
            assertEquals(5, g1.value());
            assertEquals(3, g2.value());

            // Update single broker directly
            metrics.setUrpsForBroker(1, 7);
            assertEquals(7, g1.value());

            // Bulk update that removes broker 2 and adds broker 3
            java.util.Map<Integer, Integer> counts2 = new java.util.HashMap<>();
            counts2.put(1, 1);
            counts2.put(3, 9);
            metrics.updateUrpsByBroker(counts2);

            assertEquals(1, ((Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(1))).value());
            assertNull(registry.allMetrics().get(urpsMetricNameForBroker(2)), "broker 2 gauge should be removed");
            assertEquals(9, ((Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(3))).value());

            // Explicit removal
            metrics.removeUrpsMetricsForBroker(1);
            assertNull(registry.allMetrics().get(urpsMetricNameForBroker(1)));
        } finally {
            registry.shutdown();
        }
    }
}
