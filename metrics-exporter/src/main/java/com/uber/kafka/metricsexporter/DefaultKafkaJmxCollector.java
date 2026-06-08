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

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.PrometheusNaming;

/**
 * Default JMX collector that auto-discovers all MBeans and exports generic metrics
 * for any MBeans NOT already covered by the YAML whitelist (handled by JmxCollector)
 * and NOT matching the blacklist.
 *
 * This ensures no metric overlap: a MBean is handled by exactly one system.
 * Only simple numeric attributes are exported; CompositeData, TabularData, and
 * Boolean are skipped. Label naming follows the YAML config (lowercase) for consistency.
 *
 * Connection model: when constructed with a JMX URL, opens a fresh JMXConnector per
 * scrape (matches vanilla io.prometheus.jmx.JmxScraper). This lets the exporter start
 * before Kafka is up and recover automatically across broker restarts. When constructed
 * with an explicit MBeanServerConnection (test usage), that connection is reused as-is.
 */
public class DefaultKafkaJmxCollector implements MultiCollector {

    private final String jmxUrl;
    private final MBeanServerConnection injectedMbeanServer;
    private final List<ObjectName> whitelistPatterns;
    private final List<ObjectName> blacklistPatterns;

    /**
     * Production constructor: opens a fresh JMX connection on every scrape. An empty
     * or null jmxUrl falls back to the in-process platform MBean server, matching
     * vanilla JmxScraper semantics.
     *
     * @param jmxUrl             JMX service URL (e.g. "service:jmx:rmi:///jndi/rmi://..."),
     *                           or null/empty for the in-process MBean server
     * @param whitelistPatterns  ObjectName patterns to skip (already handled by JmxCollector)
     * @param blacklistPatterns  ObjectName patterns to exclude from fallback collection
     */
    public DefaultKafkaJmxCollector(String jmxUrl,
                                    List<ObjectName> whitelistPatterns,
                                    List<ObjectName> blacklistPatterns) {
        this.jmxUrl = jmxUrl;
        this.injectedMbeanServer = null;
        this.whitelistPatterns = whitelistPatterns != null ? whitelistPatterns : Collections.emptyList();
        this.blacklistPatterns = blacklistPatterns != null ? blacklistPatterns : Collections.emptyList();
    }

    /**
     * Test constructor: reuses the supplied MBeanServerConnection on every scrape. Used
     * by unit tests to inject a mocked MBeanServerConnection.
     *
     * @param mbeanServer        JMX connection to query MBeans from
     * @param whitelistPatterns  ObjectName patterns to skip (already handled by JmxCollector)
     * @param blacklistPatterns  ObjectName patterns to exclude from fallback collection
     */
    public DefaultKafkaJmxCollector(MBeanServerConnection mbeanServer,
                                    List<ObjectName> whitelistPatterns,
                                    List<ObjectName> blacklistPatterns) {
        this.jmxUrl = null;
        this.injectedMbeanServer = mbeanServer;
        this.whitelistPatterns = whitelistPatterns != null ? whitelistPatterns : Collections.emptyList();
        this.blacklistPatterns = blacklistPatterns != null ? blacklistPatterns : Collections.emptyList();
    }

    /**
     * Parse whitelistObjectNames from a jmx-exporter YAML config file.
     */
    public static List<ObjectName> parseWhitelistFromYaml(String yamlConfigPath) {
        return parseObjectNamesFromYaml(yamlConfigPath, "whitelistObjectNames");
    }

    /**
     * Parse blacklistObjectNames from a jmx-exporter YAML config file.
     */
    public static List<ObjectName> parseBlacklistFromYaml(String yamlConfigPath) {
        return parseObjectNamesFromYaml(yamlConfigPath, "blacklistObjectNames");
    }

    /**
     * Load YAML config and parse a list of ObjectName patterns from the given config key.
     */
    @SuppressWarnings("unchecked")
    private static List<ObjectName> parseObjectNamesFromYaml(String yamlConfigPath, String configKey) {
        List<ObjectName> patterns = new ArrayList<>();
        try (InputStream is = Files.newInputStream(Paths.get(yamlConfigPath))) {
            Map<String, Object> config = new Yaml().load(is);
            if (config == null) {
                return patterns;
            }
            List<String> entries = (List<String>) config.get(configKey);
            if (entries != null) {
                for (String entry : entries) {
                    try {
                        patterns.add(new ObjectName(entry));
                    } catch (MalformedObjectNameException e) {
                        System.err.println("Skipping malformed " + configKey + " pattern: "
                                + entry + " (" + e.getMessage() + ")");
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to parse YAML config: " + e.getMessage());
            e.printStackTrace();
        }
        return patterns;
    }

    public DefaultKafkaJmxCollector register() {
        return register(PrometheusRegistry.defaultRegistry);
    }

    public DefaultKafkaJmxCollector register(PrometheusRegistry registry) {
        registry.register(this);
        return this;
    }

    private boolean isWhitelisted(ObjectName mbeanName) {
        for (ObjectName pattern : whitelistPatterns) {
            if (pattern.apply(mbeanName)) {
                return true;
            }
        }
        return false;
    }

    private boolean isBlacklisted(ObjectName mbeanName) {
        for (ObjectName pattern : blacklistPatterns) {
            if (pattern.apply(mbeanName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public MetricSnapshots collect() {
        // Test-injected connection: reuse it directly, no connect/close per scrape.
        if (injectedMbeanServer != null) {
            return collectFrom(injectedMbeanServer);
        }
        // Production: open fresh per scrape, mirroring vanilla JmxScraper.doScrape().
        if (jmxUrl == null || jmxUrl.isEmpty()) {
            return collectFrom(ManagementFactory.getPlatformMBeanServer());
        }
        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), null)) {
            return collectFrom(connector.getMBeanServerConnection());
        } catch (Exception e) {
            System.err.println("DefaultKafkaJmxCollector connect error: " + e.getMessage());
            return new MetricSnapshots(Collections.emptyList());
        }
    }

    private MetricSnapshots collectFrom(MBeanServerConnection mbeanServer) {
        Map<String, GaugeSnapshot.Builder> gauges = new LinkedHashMap<>();

        try {
            Set<ObjectName> allMBeans = mbeanServer.queryNames(null, null);

            for (ObjectName objectName : allMBeans) {
                if (isWhitelisted(objectName) || isBlacklisted(objectName)) {
                    continue;
                }
                try {
                    collectMBean(mbeanServer, objectName, gauges);
                } catch (Exception e) {
                    // Skip failed MBeans silently
                }
            }
        } catch (Exception e) {
            System.err.println("DefaultKafkaJmxCollector error: " + e.getMessage());
        }

        List<MetricSnapshot> snapshots = gauges.values().stream()
                .map(GaugeSnapshot.Builder::build)
                .collect(Collectors.toList());
        return new MetricSnapshots(snapshots);
    }

    private void collectMBean(MBeanServerConnection mbeanServer,
                              ObjectName objectName,
                              Map<String, GaugeSnapshot.Builder> gauges) throws Exception {
        MBeanInfo info = mbeanServer.getMBeanInfo(objectName);

        String domain = objectName.getDomain().replace('.', '_');
        Hashtable<String, String> props = objectName.getKeyPropertyList();

        StringBuilder baseNameBuilder = new StringBuilder(domain);
        String type = props.get("type");
        if (type != null) {
            baseNameBuilder.append("_").append(type);
        }
        String name = props.get("name");
        if (name != null) {
            baseNameBuilder.append("_").append(name);
        }
        String baseName = PrometheusNaming.sanitizeMetricName(
                normalizeName(baseNameBuilder.toString()));

        // Build labels from ObjectName properties, excluding domain/type/name
        // since those are already encoded in the metric name.
        Labels.Builder labelsBuilder = Labels.builder();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (!key.equals("type") && !key.equals("name")) {
                labelsBuilder.label(normalizeLabel(key), entry.getValue().toLowerCase());
            }
        }
        Labels labels = labelsBuilder.build();

        for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
            if (!attrInfo.isReadable()) {
                continue;
            }
            String attrName = attrInfo.getName();
            Object value;
            try {
                value = mbeanServer.getAttribute(objectName, attrName);
            } catch (Exception e) {
                continue;
            }

            String metricName = PrometheusNaming.sanitizeMetricName(
                    baseName + "_" + normalizeName(attrName));
            emitValue(value, metricName, labels, gauges);
        }
    }

    private void emitValue(Object value, String metricName, Labels labels,
                           Map<String, GaugeSnapshot.Builder> gauges) {
        if (value instanceof Number) {
            double numericValue = ((Number) value).doubleValue();
            GaugeSnapshot.Builder builder = gauges.computeIfAbsent(metricName,
                    k -> GaugeSnapshot.builder().name(k).help("JMX metric (generic fallback)"));
            builder.dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                    .labels(labels)
                    .value(numericValue)
                    .build());
        } else if (value instanceof CompositeData) {
            CompositeData data = (CompositeData) value;
            for (String key : data.getCompositeType().keySet()) {
                String compositeMetricName = PrometheusNaming.sanitizeMetricName(
                        metricName + "_" + normalizeName(key));
                emitValue(data.get(key), compositeMetricName, labels, gauges);
            }
        }
        // Skip unsupported types (String, Boolean, TabularData, etc.)
    }

    private String normalizeName(String name) {
        String normalized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        normalized = normalized.toLowerCase();
        normalized = normalized.replaceAll("_+", "_");
        normalized = normalized.replaceAll("^_+|_+$", "");
        return normalized;
    }

    private String normalizeLabel(String label) {
        return label.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }
}
