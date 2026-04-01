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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import io.prometheus.jmx.BuildInfoMetrics;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;

/**
 * Kafka JMX to Prometheus Exporter
 *
 * Combines two collection strategies:
 * 1. JmxCollector (from jmx_prometheus_httpserver 1.0.1) — handles YAML-configured metrics
 * 2. DefaultKafkaJmxCollector — auto-discovers all remaining MBeans not covered by the YAML whitelist
 *
 * Uses the same jmx_prometheus_httpserver library as the vanilla exporter, ensuring
 * identical metric names.
 */
public class KafkaJmxExporter {

    static final int DEFAULT_HTTP_PORT = 7071;

    public static void main(String[] args) throws Exception {
        Namespace parsedArgs = parseArgs(args);
        if (parsedArgs == null) {
            System.err.println("Failed to parse arguments. Exiting.");
            return;
        }

        String yamlConfigPath = parsedArgs.getString("config");
        int httpPort = parsedArgs.getInt("http_port");

        System.out.println("Kafka JMX to Prometheus Exporter");
        System.out.println("========================================");
        System.out.println("YAML config: " + yamlConfigPath);
        System.out.println("HTTP Port:   " + httpPort);

        startHybridMode(yamlConfigPath, httpPort);

        System.out.println();
        System.out.println("Exporter started successfully!");
        System.out.println("Metrics endpoint: http://localhost:" + httpPort + "/metrics");
        System.out.println("Press Ctrl+C to stop");

        Thread.currentThread().join();
    }

    /**
     * Hybrid mode: YAML rules (via JmxCollector) + generic fallback for unmatched MBeans.
     */
    private static void startHybridMode(String yamlConfigPath, int httpPort) throws Exception {
        // Read YAML as-is — sed has already replaced the JMX_PORT placeholder before launch
        String yamlContent = new String(Files.readAllBytes(new File(yamlConfigPath).toPath()), StandardCharsets.UTF_8);

        String jmxUrl = extractJmxConnectionUrl(yamlContent);

        System.out.println("JMX URL:     " + (jmxUrl != null ? jmxUrl : "(local MBeanServer)"));
        System.out.println();

        // JmxCollector manages its own JMX connection from the YAML's jmxUrl.
        // We create a separate connection for DefaultKafkaJmxCollector.
        MBeanServerConnection mbeanServer = connectToJmx(jmxUrl);

        // Register collectors — same as vanilla exporter, plus our fallback
        new BuildInfoMetrics().register();
        new JmxCollector(yamlContent).register();
        System.out.println("Registered JmxCollector with YAML config: " + yamlConfigPath);

        List<ObjectName> whitelist = DefaultKafkaJmxCollector.parseWhitelistFromYaml(yamlConfigPath);
        List<ObjectName> blacklist = DefaultKafkaJmxCollector.parseBlacklistFromYaml(yamlConfigPath);
        new DefaultKafkaJmxCollector(mbeanServer, whitelist, blacklist).register();
        System.out.println("Registered DefaultKafkaJmxCollector (" + whitelist.size()
                + " whitelist, " + blacklist.size() + " blacklist patterns, collecting unmatched MBeans)");

        // Start HTTP server (uses PrometheusRegistry.defaultRegistry automatically)
        HTTPServer.builder().port(httpPort).buildAndStart();
    }

    private static MBeanServerConnection connectToJmx(String jmxUrl) throws Exception {
        if (jmxUrl != null) {
            System.out.println("Connecting to JMX at " + jmxUrl);
            JMXServiceURL serviceUrl = new JMXServiceURL(jmxUrl);
            JMXConnector connector = JMXConnectorFactory.connect(serviceUrl, null);
            return connector.getMBeanServerConnection();
        } else {
            System.out.println("Using local MBeanServer (in-process)");
            return ManagementFactory.getPlatformMBeanServer();
        }
    }

    static String extractJmxConnectionUrl(String yamlContent) {
        Matcher m = Pattern.compile("jmxUrl:\\s*\"([^\"]+)\"").matcher(yamlContent);
        return m.find() ? m.group(1) : null;
    }

    static ArgumentParser buildParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-jmx-exporter")
                .defaultHelp(true)
                .description("Exports Kafka JMX metrics to Prometheus. "
                        + "Metrics matching the YAML rules are exported with precise names "
                        + "(preserving existing dashboard/alert compatibility), and all remaining "
                        + "MBeans are exported automatically with generic labels. "
                        + "The JMX connection URL is derived from the YAML config's jmxUrl field.");

        parser.addArgument("--config", "-c")
                .required(true)
                .help("Path to jmx-exporter YAML config file. "
                        + "The YAML must have jmxUrl set. "
                        + "YAML rules handle whitelisted MBeans and a generic fallback handles everything else.");

        parser.addArgument("--http-port", "-p")
                .type(Integer.class)
                .setDefault(DEFAULT_HTTP_PORT)
                .help("HTTP port to serve Prometheus metrics on. Default: " + DEFAULT_HTTP_PORT);

        return parser;
    }

    static Namespace parseArgs(String[] args) {
        ArgumentParser parser = buildParser();
        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return null;
        }
    }
}
