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

import com.sun.net.httpserver.HttpServer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import io.prometheus.jmx.BuildInfoMetrics;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.metrics.exporter.httpserver.MetricsHandler;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/**
 * Kafka JMX to Prometheus Exporter
 *
 * Combines two collection strategies, served on two paths of the same HTTP port to keep
 * scrape volume manageable for downstream collectors:
 * 1. JmxCollector (from jmx_prometheus_httpserver 1.0.1) — handles YAML-configured metrics
 *    (served at /metrics).
 * 2. DefaultKafkaJmxCollector — auto-discovers all remaining MBeans not covered by the YAML
 *    whitelist (served at /metrics/fallback).
 *
 * Each path is backed by its own PrometheusRegistry so the two collector outputs are fully
 * isolated.
 */
public class KafkaJmxExporter {

    static final int DEFAULT_HTTP_PORT = 7071;
    static final String CONFIG_METRICS_PATH = "/metrics";
    static final String FALLBACK_METRICS_PATH = "/metrics/fallback";

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
        System.out.println("Config metrics endpoint:   http://localhost:" + httpPort + CONFIG_METRICS_PATH);
        System.out.println("Fallback metrics endpoint: http://localhost:" + httpPort + FALLBACK_METRICS_PATH);
        System.out.println("Press Ctrl+C to stop");

        Thread.currentThread().join();
    }

    /**
     * Hybrid mode: YAML rules (via JmxCollector) at /metrics, generic fallback for unmatched
     * MBeans at /metrics/fallback, both on the same port. Each path is backed by its own
     * PrometheusRegistry so the two collector outputs are fully isolated.
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

        // Config-based metrics → primary registry → /metrics
        PrometheusRegistry configRegistry = new PrometheusRegistry();
        new BuildInfoMetrics().register(configRegistry);
        new JmxCollector(yamlContent).register(configRegistry);
        System.out.println("Registered JmxCollector with YAML config: " + yamlConfigPath
                + " (path " + CONFIG_METRICS_PATH + ")");

        // Catch-all fallback metrics → fallback registry → /metrics/fallback
        PrometheusRegistry fallbackRegistry = new PrometheusRegistry();
        List<ObjectName> whitelist = DefaultKafkaJmxCollector.parseWhitelistFromYaml(yamlConfigPath);
        List<ObjectName> blacklist = DefaultKafkaJmxCollector.parseBlacklistFromYaml(yamlConfigPath);
        new DefaultKafkaJmxCollector(mbeanServer, whitelist, blacklist).register(fallbackRegistry);
        System.out.println("Registered DefaultKafkaJmxCollector (" + whitelist.size()
                + " whitelist, " + blacklist.size() + " blacklist patterns, collecting unmatched MBeans)"
                + " (path " + FALLBACK_METRICS_PATH + ")");

        // Single HTTP server, two paths — one MetricsHandler per registry. Longest-prefix
        // match means /metrics/fallback hits the fallback handler, everything else under
        // /metrics hits the config handler.
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext(CONFIG_METRICS_PATH, new MetricsHandler(configRegistry));
        server.createContext(FALLBACK_METRICS_PATH, new MetricsHandler(fallbackRegistry));
        server.setExecutor(Executors.newFixedThreadPool(5, daemonThreadFactory()));
        server.start();
    }

    private static ThreadFactory daemonThreadFactory() {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, "kafka-jmx-exporter-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
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
                .description("Exports Kafka JMX metrics to Prometheus on two HTTP paths of the same port. "
                        + "Metrics matching the YAML rules are exported with precise names "
                        + "(preserving existing dashboard/alert compatibility) at /metrics, "
                        + "and all remaining MBeans are exported automatically with generic labels "
                        + "at /metrics/fallback. Splitting them keeps individual scrape responses "
                        + "small enough to avoid collector timeouts. "
                        + "The JMX connection URL is derived from the YAML config's jmxUrl field.");

        parser.addArgument("--config", "-c")
                .required(true)
                .help("Path to jmx-exporter YAML config file. "
                        + "The YAML must have jmxUrl set. "
                        + "YAML rules handle whitelisted MBeans and a generic fallback handles everything else.");

        parser.addArgument("--http-port", "-p")
                .type(Integer.class)
                .setDefault(DEFAULT_HTTP_PORT)
                .help("HTTP port to serve Prometheus metrics on. Both /metrics (YAML/config-based) "
                        + "and /metrics/fallback (catch-all) are served on this port. "
                        + "Default: " + DEFAULT_HTTP_PORT);

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
