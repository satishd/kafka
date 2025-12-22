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
package org.apache.kafka.rsm.hdfs.prefetch;

import org.apache.kafka.rsm.hdfs.LogSegmentDataHeader;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RSMTestUtils {

    public static void writeData(FileChannel fileChannel, byte[] data) throws IOException {
        // Create a ByteBuffer for the header
        ByteBuffer headerBuffer = ByteBuffer.allocate(LogSegmentDataHeader.LENGTH);
        // Set version byte
        headerBuffer.put(LogSegmentDataHeader.CURRENT_VERSION);

        // Calculate positions for all file types
        int startPos = LogSegmentDataHeader.LENGTH;
        int[] positions = new int[LogSegmentDataHeader.FileType.values().length];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = startPos;
            if (i < positions.length - 1) { // All except the last one (SEGMENT)
                startPos += 10; // Arbitrary size for non-segment files
            }
        }

        // Write positions to the header
        for (int pos : positions) {
            headerBuffer.putInt(pos);
        }

        // Reset buffer position for writing
        headerBuffer.flip();

        // Write the header to the file
        fileChannel.write(headerBuffer);

        // Write dummy data for non-segment files
        for (int i = 0; i < positions.length - 1; i++) {
            fileChannel.position(positions[i]);
            fileChannel.write(ByteBuffer.wrap(new byte[10])); // 10 bytes of zeros
        }

        // Write the test data at the SEGMENT position
        int segmentPos = positions[positions.length - 1]; // Position for SEGMENT
        fileChannel.position(segmentPos);
        fileChannel.write(ByteBuffer.wrap(data));
        fileChannel.position(0);
    }

    public static void clearKafkaMetrics() {
        KafkaYammerMetrics.defaultRegistry().allMetrics().forEach(
            (metricName, metric) -> KafkaYammerMetrics.defaultRegistry().removeMetric(metricName));
    }

    public static Optional<Metric> findKafkaMetric(Class<?> kclass, String name) {
        return findKafkaMetric(kclass, name, Collections.emptyMap());
    }

    private static Optional<Metric> findKafkaMetric(Class<?> kclass, String name, Map<String, String> tags) {
        String scope = tags.entrySet().stream().map(e -> e.getKey() + "." + e.getValue()).collect(Collectors.joining(","));
        return KafkaYammerMetrics.defaultRegistry().allMetrics()
            .entrySet()
            .stream()
            .filter(entry -> {
                MetricName metricName = entry.getKey();
                return metricName.getGroup().equals(kclass.getPackage().getName()) &&
                    metricName.getType().equals(kclass.getSimpleName()) &&
                    metricName.getName().equals(name) && (!metricName.hasScope() || metricName.getScope().equals(scope));
            })
            .findFirst()
            .map(Map.Entry::getValue);
    }

    public static long timerCount(Class<?> kclass, String name) {
        return timerCount(kclass, name, Collections.emptyMap());
    }

    public static long timerCount(Class<?> kclass, String name, Map<String, String> tags) {
        Timer timer = findKafkaMetric(kclass, name, tags)
                .map(metric -> (Timer) metric)
                .orElseThrow(() -> new AssertionError("Metric " + name + " with tags " + tags + " not found"));
        return timer.count();
    }

    public static void verifyTimerCount(Class<?> kclass, String name, long expectedValue) {
        verifyTimerCount(kclass, name, Collections.emptyMap(), expectedValue);
    }

    public static void verifyTimerCount(Class<?> kclass, String name, Map<String, String> tags, long expectedValue) {
        long actualValue = timerCount(kclass, name, tags);
        assertEquals(expectedValue, actualValue, "Timer count check failed for " + name + " with tags " + tags);
    }

    public static void verifyTimerQuantile(Class<?> klass, String name, double quantile, Predicate<Double> assertion) {
        verifyTimerQuantile(klass, name, Collections.emptyMap(), quantile, assertion);
    }

    public static void verifyTimerQuantile(Class<?> klass, String name, Map<String, String> tags, double quantile, Predicate<Double> assertion) {
        Timer timer = findKafkaMetric(klass, name, tags)
            .map(metric -> (Timer) metric)
            .orElseThrow(() -> new AssertionError("Metric " + name + " not found"));

        double value = timer.getSnapshot().getValue(quantile);
        assertTrue(assertion.test(value), "Timer quantile check failed for " + name);
    }

    public static void verifyMeter(Class<?> kclass, String name, long expectedCount) {
        verifyMeter(kclass, name, Collections.emptyMap(), expectedCount);
    }

    public static void verifyMeter(Class<?> klass, String name, Map<String, String> tags, long expectedCount) {
        Meter meter = findKafkaMetric(klass, name, tags)
            .map(metric -> (Meter) metric)
            .orElseThrow(() -> new AssertionError("Meter " + name + " with tags " + tags + " not found"));
        assertEquals(expectedCount, meter.count(), "Meter count check failed for " + name + " with tags " + tags);
    }

    public static void verifyMeterWithTimeout(Duration timeout, Class<?> klass, String name, long expectedCount) {
        verifyMeterWithTimeout(timeout, klass, name, Collections.emptyMap(), expectedCount);
    }

    public static void verifyMeterWithTimeout(Duration timeout, Class<?> klass, String name, Map<String, String> tags, long expectedCount) {
        Meter meter = findKafkaMetric(klass, name, tags)
            .map(metric -> (Meter) metric)
            .orElseThrow(() -> new AssertionError("Meter " + name + " with tags " + tags + " not found"));

        assertTimeoutPreemptively(
            timeout,
            () -> {
                while (meter.count() != expectedCount) {
                    Thread.sleep(50);
                }
            }
        );
    }


    public static <T> void verifyGauge(Class<?> klass, String name, T expectedValue) {
        Optional<Metric> metric = findKafkaMetric(klass, name)
            .filter(m -> m instanceof Gauge<?>);

        assertTrue(metric.isPresent(), "Metric " + name + " not found or not a Gauge");
        Object actualValue = ((Gauge<?>) metric.get()).value();
        assertEquals(expectedValue, actualValue, "Gauge value mismatch for " + name);
    }
}
