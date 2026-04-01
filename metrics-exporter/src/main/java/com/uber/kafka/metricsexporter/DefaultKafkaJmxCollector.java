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
 */
public class DefaultKafkaJmxCollector implements MultiCollector {

    private final MBeanServerConnection mbeanServer;
    private final List<ObjectName> whitelistPatterns;
    private final List<ObjectName> blacklistPatterns;

    /**
     * @param mbeanServer        JMX connection to query MBeans from
     * @param whitelistPatterns  ObjectName patterns to skip (already handled by JmxCollector)
     * @param blacklistPatterns  ObjectName patterns to exclude from fallback collection
     */
    public DefaultKafkaJmxCollector(MBeanServerConnection mbeanServer,
                                    List<ObjectName> whitelistPatterns,
                                    List<ObjectName> blacklistPatterns) {
        this.mbeanServer = mbeanServer;
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
        Map<String, GaugeSnapshot.Builder> gauges = new LinkedHashMap<>();

        try {
            Set<ObjectName> allMBeans = mbeanServer.queryNames(null, null);

            for (ObjectName objectName : allMBeans) {
                if (isWhitelisted(objectName) || isBlacklisted(objectName)) {
                    continue;
                }
                try {
                    collectMBean(objectName, gauges);
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

    private void collectMBean(ObjectName objectName,
                              Map<String, GaugeSnapshot.Builder> gauges) throws Exception {
        MBeanInfo info = mbeanServer.getMBeanInfo(objectName);

        String domain = objectName.getDomain().replace('.', '_');
        Hashtable<String, String> props = objectName.getKeyPropertyList();

        String type = props.getOrDefault("type", "unknown");
        String name = props.getOrDefault("name", "unknown");
        String baseName = PrometheusNaming.sanitizeMetricName(
                normalizeName(domain + "_" + type + "_" + name));

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
