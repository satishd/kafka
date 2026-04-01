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
package com.uber.kafka.metricsexporter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.SimpleType;

import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultKafkaJmxCollectorTest {

    private MBeanServerConnection mbeanServer;

    @BeforeEach
    void setUp() {
        mbeanServer = mock(MBeanServerConnection.class);
    }

    // -------- YAML parsing tests --------

    @Test
    void parseWhitelistFromYaml_returnsPatterns(@TempDir Path tmpDir) throws Exception {
        File configFile = tmpDir.resolve("config.yaml").toFile();
        try (FileWriter w = new FileWriter(configFile)) {
            w.write("whitelistObjectNames:\n");
            w.write("  - 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*'\n");
            w.write("  - 'java.lang:type=GarbageCollector,name=*'\n");
        }

        List<ObjectName> patterns = DefaultKafkaJmxCollector.parseWhitelistFromYaml(configFile.getAbsolutePath());
        assertEquals(2, patterns.size());
        assertEquals(new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*"), patterns.get(0));
        assertEquals(new ObjectName("java.lang:type=GarbageCollector,name=*"), patterns.get(1));
    }

    @Test
    void parseBlacklistFromYaml_returnsPatterns(@TempDir Path tmpDir) throws Exception {
        File configFile = tmpDir.resolve("config.yaml").toFile();
        try (FileWriter w = new FileWriter(configFile)) {
            w.write("blacklistObjectNames:\n");
            w.write("  - '*:name=BytesRejectedPerSec,*'\n");
        }

        List<ObjectName> patterns = DefaultKafkaJmxCollector.parseBlacklistFromYaml(configFile.getAbsolutePath());
        assertEquals(1, patterns.size());
    }

    @Test
    void parseWhitelistFromYaml_emptyFile(@TempDir Path tmpDir) throws Exception {
        File configFile = tmpDir.resolve("empty.yaml").toFile();
        try (FileWriter w = new FileWriter(configFile)) {
            w.write("");
        }

        List<ObjectName> patterns = DefaultKafkaJmxCollector.parseWhitelistFromYaml(configFile.getAbsolutePath());
        assertTrue(patterns.isEmpty());
    }

    @Test
    void parseWhitelistFromYaml_noWhitelistKey(@TempDir Path tmpDir) throws Exception {
        File configFile = tmpDir.resolve("no_wl.yaml").toFile();
        try (FileWriter w = new FileWriter(configFile)) {
            w.write("startDelaySeconds: 0\n");
        }

        List<ObjectName> patterns = DefaultKafkaJmxCollector.parseWhitelistFromYaml(configFile.getAbsolutePath());
        assertTrue(patterns.isEmpty());
    }

    @Test
    void parseWhitelistFromYaml_malformedPatternSkipped(@TempDir Path tmpDir) throws Exception {
        File configFile = tmpDir.resolve("bad.yaml").toFile();
        try (FileWriter w = new FileWriter(configFile)) {
            w.write("whitelistObjectNames:\n");
            w.write("  - ':::invalid:::'\n");
            w.write("  - 'java.lang:type=GarbageCollector,name=*'\n");
        }

        List<ObjectName> patterns = DefaultKafkaJmxCollector.parseWhitelistFromYaml(configFile.getAbsolutePath());
        assertEquals(1, patterns.size());
    }

    @Test
    void parseWhitelistFromYaml_nonExistentFile() {
        List<ObjectName> patterns = DefaultKafkaJmxCollector.parseWhitelistFromYaml("/nonexistent/path.yaml");
        assertTrue(patterns.isEmpty());
    }

    // -------- isWhitelisted / isBlacklisted via collect() --------

    @Test
    void collect_wildcardWhitelistMatchesConcreteBean() throws Exception {
        // Wildcard pattern topic=* should match any concrete topic
        ObjectName wildcardWhitelisted = new ObjectName(
                "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=abc");
        // Exact-match whitelist (no wildcard) should also work
        ObjectName exactWhitelisted = new ObjectName("kafka.server:type=raft-metrics");
        ObjectName notWhitelisted = new ObjectName("kafka.log:type=Log,name=LogSize");

        List<ObjectName> whitelistPatterns = Arrays.asList(
                new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*"),
                new ObjectName("kafka.server:type=raft-metrics"));

        when(mbeanServer.queryNames(null, null))
                .thenReturn(new HashSet<>(Arrays.asList(
                        wildcardWhitelisted, exactWhitelisted, notWhitelisted)));

        MBeanInfo logInfo = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(notWhitelisted)).thenReturn(logInfo);
        when(mbeanServer.getAttribute(notWhitelisted, "Value")).thenReturn(42.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, whitelistPatterns, Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        // Whitelisted beans (both wildcard and exact) should be skipped
        verify(mbeanServer, never()).getMBeanInfo(wildcardWhitelisted);
        verify(mbeanServer, never()).getMBeanInfo(exactWhitelisted);
        // Non-whitelisted bean should be collected
        assertTrue(snapshots.size() > 0);
    }

    @Test
    void collect_skipsWhitelistedMBeans() throws Exception {
        ObjectName whitelistedBean = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=test");
        ObjectName nonWhitelistedBean = new ObjectName("kafka.log:type=Log,name=LogSize");
        List<ObjectName> whitelistPatterns = Collections.singletonList(
                new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*"));

        when(mbeanServer.queryNames(null, null))
                .thenReturn(new HashSet<>(Arrays.asList(whitelistedBean, nonWhitelistedBean)));

        MBeanInfo logInfo = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(nonWhitelistedBean)).thenReturn(logInfo);
        when(mbeanServer.getAttribute(nonWhitelistedBean, "Value")).thenReturn(42.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, whitelistPatterns, Collections.emptyList());

        MetricSnapshots snapshots = collector.collect();

        // Should NOT query MBeanInfo for the whitelisted bean
        verify(mbeanServer, never()).getMBeanInfo(whitelistedBean);
        // Should have collected the non-whitelisted bean
        assertTrue(snapshots.size() > 0);
    }

    @Test
    void collect_skipsBlacklistedMBeans() throws Exception {
        ObjectName blacklistedBean = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=test");
        ObjectName normalBean = new ObjectName("kafka.log:type=Log,name=LogSize");
        List<ObjectName> blacklistPatterns = Collections.singletonList(
                new ObjectName("*:name=BytesRejectedPerSec,*"));

        when(mbeanServer.queryNames(null, null))
                .thenReturn(new HashSet<>(Arrays.asList(blacklistedBean, normalBean)));

        MBeanInfo logInfo = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(normalBean)).thenReturn(logInfo);
        when(mbeanServer.getAttribute(normalBean, "Value")).thenReturn(10.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), blacklistPatterns);

        MetricSnapshots snapshots = collector.collect();

        verify(mbeanServer, never()).getMBeanInfo(blacklistedBean);
        assertTrue(snapshots.size() > 0);
    }

    // -------- Metric type classification --------

    @Test
    void collect_numericAttributeAsGauge() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=ReplicaManager,name=LeaderCount");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "Value")).thenReturn(5.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        boolean foundGauge = false;
        for (MetricSnapshot snapshot : snapshots) {
            if (snapshot instanceof GaugeSnapshot) {
                foundGauge = true;
                GaugeSnapshot gauge = (GaugeSnapshot) snapshot;
                assertEquals(5.0, gauge.getDataPoints().get(0).getValue());
            }
        }
        assertTrue(foundGauge, "Expected a gauge metric for 'Value' attribute");
    }

    @Test
    void collect_countAttributeAsGauge() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=ReplicaManager,name=LeaderCount");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Count", "long", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "Count")).thenReturn(100L);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        boolean foundGauge = false;
        for (MetricSnapshot snapshot : snapshots) {
            if (snapshot instanceof GaugeSnapshot) {
                foundGauge = true;
                GaugeSnapshot gauge = (GaugeSnapshot) snapshot;
                assertEquals(100.0, gauge.getDataPoints().get(0).getValue());
            }
        }
        assertTrue(foundGauge, "Expected a gauge metric for 'Count' attribute");
    }

    @Test
    void collect_attributeEndingWithCountIsGauge() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=KafkaServer,name=Connections");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("TotalCount", "long", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "TotalCount")).thenReturn(50L);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        boolean foundGauge = false;
        for (MetricSnapshot snapshot : snapshots) {
            if (snapshot instanceof GaugeSnapshot) {
                foundGauge = true;
            }
        }
        assertTrue(foundGauge, "Expected gauge for attribute ending with 'Count'");
    }

    // -------- CompositeData handling --------

    @Test
    void collect_compositeDataAttribute() throws Exception {
        ObjectName bean = new ObjectName("java.lang:type=Memory");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
            new MBeanAttributeInfo("HeapMemoryUsage", "javax.management.openmbean.CompositeData",
                "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);

        CompositeType ct = new CompositeType("memory", "Memory usage",
            new String[]{"used", "max"},
            new String[]{"Used memory", "Max memory"},
            new javax.management.openmbean.OpenType[]{SimpleType.LONG, SimpleType.LONG});
        CompositeDataSupport cd = new CompositeDataSupport(ct,
            new String[]{"used", "max"},
            new Object[]{1024L, 4096L});
        when(mbeanServer.getAttribute(bean, "HeapMemoryUsage")).thenReturn(cd);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
            mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertEquals(2, snapshots.size());
        Set<String> metricNames = new HashSet<>();
        for (MetricSnapshot snapshot : snapshots) {
            GaugeSnapshot gauge = (GaugeSnapshot) snapshot;
            metricNames.add(gauge.getMetadata().getName());
            if (gauge.getMetadata().getName().equals("java_lang_memory_unknown_heapmemoryusage_used")) {
                assertEquals(1024.0, gauge.getDataPoints().get(0).getValue());
            }
            if (gauge.getMetadata().getName().equals("java_lang_memory_unknown_heapmemoryusage_max")) {
                assertEquals(4096.0, gauge.getDataPoints().get(0).getValue());
            }
        }
        assertTrue(metricNames.contains("java_lang_memory_unknown_heapmemoryusage_used"),
            "Expected metric 'java_lang_memory_unknown_heapmemoryusage_used'");
        assertTrue(metricNames.contains("java_lang_memory_unknown_heapmemoryusage_max"),
            "Expected metric 'java_lang_memory_unknown_heapmemoryusage_max'");
    }

    @Test
    void collect_compositeDataSkipsNonNumericKeys() throws Exception {
        ObjectName bean = new ObjectName("java.lang:type=Runtime");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
            new MBeanAttributeInfo("Info", "javax.management.openmbean.CompositeData",
                "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);

        CompositeType ct = new CompositeType("info", "Runtime info",
            new String[]{"uptime", "vmName"},
            new String[]{"Uptime ms", "VM name"},
            new javax.management.openmbean.OpenType[]{SimpleType.LONG, SimpleType.STRING});
        CompositeDataSupport cd = new CompositeDataSupport(ct,
            new String[]{"uptime", "vmName"},
            new Object[]{123456L, "OpenJDK"});
        when(mbeanServer.getAttribute(bean, "Info")).thenReturn(cd);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
            mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        // Only "uptime" (numeric) should produce a metric; "vmName" (String) should be skipped
        // Note: PrometheusNaming.sanitizeMetricName strips the "_info" suffix (reserved by Prometheus),
        // so the attr name "Info" is not present in the final metric name.
        assertEquals(1, snapshots.size());
        GaugeSnapshot gauge = (GaugeSnapshot) snapshots.get(0);
        assertEquals("java_lang_runtime_unknown_uptime", gauge.getMetadata().getName());
        assertEquals(123456.0, gauge.getDataPoints().get(0).getValue());
    }

    @Test
    void collect_nestedCompositeData() throws Exception {
        ObjectName bean = new ObjectName("test:type=Nested");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
            new MBeanAttributeInfo("Stats", "javax.management.openmbean.CompositeData",
                "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);

        // Inner composite
        CompositeType innerType = new CompositeType("inner", "Inner",
            new String[]{"count"},
            new String[]{"Count"},
            new javax.management.openmbean.OpenType[]{SimpleType.LONG});
        CompositeDataSupport innerCd = new CompositeDataSupport(innerType,
            new String[]{"count"},
            new Object[]{42L});

        // Outer composite containing the inner composite + a numeric field
        CompositeType outerType = new CompositeType("outer", "Outer",
            new String[]{"total", "details"},
            new String[]{"Total", "Details"},
            new javax.management.openmbean.OpenType[]{SimpleType.DOUBLE, innerType});
        CompositeDataSupport outerCd = new CompositeDataSupport(outerType,
            new String[]{"total", "details"},
            new Object[]{99.0, innerCd});

        when(mbeanServer.getAttribute(bean, "Stats")).thenReturn(outerCd);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
            mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertEquals(2, snapshots.size());
        Set<String> metricNames = new HashSet<>();
        for (MetricSnapshot snapshot : snapshots) {
            metricNames.add(((GaugeSnapshot) snapshot).getMetadata().getName());
        }
        // Note: PrometheusNaming.sanitizeMetricName strips "_total" suffix (reserved by Prometheus),
        // so "stats_total" becomes "test_nested_unknown_stats".
        assertTrue(metricNames.contains("test_nested_unknown_stats"),
            "Expected metric 'test_nested_unknown_stats', got: " + metricNames);
        assertTrue(metricNames.contains("test_nested_unknown_stats_details_count"),
            "Expected metric 'test_nested_unknown_stats_details_count', got: " + metricNames);
    }

    // -------- Non-numeric attributes are skipped --------

    @Test
    void collect_skipsNonNumericAttributes() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=KafkaServer,name=Info");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Version", "java.lang.String", "", true, false, false),
                new MBeanAttributeInfo("Active", "boolean", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "Version")).thenReturn("3.9.0");
        when(mbeanServer.getAttribute(bean, "Active")).thenReturn(true);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertEquals(0, snapshots.size(), "Non-numeric attributes should not produce metrics");
    }

    // -------- Labels --------

    @Test
    void collect_includesLabelsFromObjectName() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesIn,topic=myTopic");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "Value")).thenReturn(1000.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        GaugeSnapshot gauge = null;
        for (MetricSnapshot snapshot : snapshots) {
            if (snapshot instanceof GaugeSnapshot) {
                gauge = (GaugeSnapshot) snapshot;
            }
        }
        assertNotNull(gauge);

        Labels labels = gauge.getDataPoints().get(0).getLabels();
        // jmx_domain, type, and name are encoded in the metric name, not as labels
        assertFalse(labels.contains("jmx_domain"), "jmx_domain should not be a label");
        assertFalse(labels.contains("type"), "type should not be a label");
        assertFalse(labels.contains("name"), "name should not be a label");
        // Extra ObjectName properties should still appear as labels
        assertEquals("mytopic", labels.get("topic"));
    }

    // -------- Error handling --------

    @Test
    void collect_handlesQueryNamesException() throws Exception {
        when(mbeanServer.queryNames(null, null)).thenThrow(new RuntimeException("connection lost"));

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertEquals(0, snapshots.size());
    }

    @Test
    void collect_handlesFailedMBeanGracefully() throws Exception {
        ObjectName goodBean = new ObjectName("kafka.server:type=Good,name=Metric");
        ObjectName badBean = new ObjectName("kafka.server:type=Bad,name=Metric");

        when(mbeanServer.queryNames(null, null))
                .thenReturn(new LinkedHashSet<>(Arrays.asList(badBean, goodBean)));
        when(mbeanServer.getMBeanInfo(badBean)).thenThrow(new RuntimeException("bad bean"));

        MBeanInfo goodInfo = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(goodBean)).thenReturn(goodInfo);
        when(mbeanServer.getAttribute(goodBean, "Value")).thenReturn(1.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertTrue(snapshots.size() > 0, "Good bean should still produce metrics");
    }

    @Test
    void collect_handlesGetAttributeException() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=Test,name=Metric");

        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("BadAttr", "double", "", true, false, false),
                new MBeanAttributeInfo("GoodAttr", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "BadAttr")).thenThrow(new RuntimeException("unavailable"));
        when(mbeanServer.getAttribute(bean, "GoodAttr")).thenReturn(7.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertTrue(snapshots.size() > 0, "Good attribute should still produce a metric");
    }

    // -------- Null whitelist/blacklist --------

    @Test
    void constructor_acceptsNullLists() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=Test,name=Metric");
        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "Value")).thenReturn(1.0);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(mbeanServer, null, null);
        MetricSnapshots snapshots = collector.collect();
        assertTrue(snapshots.size() > 0);
    }

    // -------- Multiple MBeans and attributes --------

    @Test
    void collect_multipleMBeansMultipleAttributes() throws Exception {
        ObjectName bean1 = new ObjectName("kafka.server:type=ReplicaManager,name=LeaderCount");
        ObjectName bean2 = new ObjectName("kafka.network:type=RequestMetrics,name=Produce,request=Produce");

        when(mbeanServer.queryNames(null, null))
                .thenReturn(new LinkedHashSet<>(Arrays.asList(bean1, bean2)));

        MBeanInfo info1 = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Value", "double", "", true, false, false),
                new MBeanAttributeInfo("Count", "long", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean1)).thenReturn(info1);
        when(mbeanServer.getAttribute(bean1, "Value")).thenReturn(5.0);
        when(mbeanServer.getAttribute(bean1, "Count")).thenReturn(100L);

        MBeanInfo info2 = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("Mean", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean2)).thenReturn(info2);
        when(mbeanServer.getAttribute(bean2, "Mean")).thenReturn(12.5);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        int gaugeCount = 0;
        for (MetricSnapshot snapshot : snapshots) {
            if (snapshot instanceof GaugeSnapshot) gaugeCount++;
        }
        assertEquals(3, gaugeCount, "Expected 3 gauge metrics (Value + Count + Mean)");
    }

    // -------- Unreadable attributes --------

    @Test
    void collect_skipsUnreadableAttributes() throws Exception {
        ObjectName bean = new ObjectName("kafka.server:type=Test,name=Metric");
        when(mbeanServer.queryNames(null, null)).thenReturn(Collections.singleton(bean));

        MBeanInfo info = new MBeanInfo("", "", new MBeanAttributeInfo[]{
                new MBeanAttributeInfo("NotReadable", "double", "", false, false, false),
                new MBeanAttributeInfo("Readable", "double", "", true, false, false)
        }, null, null, null);
        when(mbeanServer.getMBeanInfo(bean)).thenReturn(info);
        when(mbeanServer.getAttribute(bean, "Readable")).thenReturn(3.14);

        DefaultKafkaJmxCollector collector = new DefaultKafkaJmxCollector(
                mbeanServer, Collections.emptyList(), Collections.emptyList());
        MetricSnapshots snapshots = collector.collect();

        assertEquals(1, snapshots.size());
        verify(mbeanServer, never()).getAttribute(bean, "NotReadable");
    }
}
