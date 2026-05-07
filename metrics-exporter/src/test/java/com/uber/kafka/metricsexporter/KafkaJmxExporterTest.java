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

import net.sourceforge.argparse4j.inf.Namespace;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaJmxExporterTest {

    // -------- extractJmxConnectionUrl --------

    @Test
    void extractJmxConnectionUrl_findsUrl() {
        String yaml = "jmxUrl: \"service:jmx:rmi:///jndi/rmi://127.0.0.1:29010/jmxrmi\"\nssl: false";
        String url = KafkaJmxExporter.extractJmxConnectionUrl(yaml);
        assertEquals("service:jmx:rmi:///jndi/rmi://127.0.0.1:29010/jmxrmi", url);
    }

    @Test
    void extractJmxConnectionUrl_noUrl() {
        String yaml = "ssl: false\nlowercaseOutputName: true";
        assertNull(KafkaJmxExporter.extractJmxConnectionUrl(yaml));
    }

    @Test
    void extractJmxConnectionUrl_withSpaces() {
        String yaml = "jmxUrl:   \"service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi\"";
        String url = KafkaJmxExporter.extractJmxConnectionUrl(yaml);
        assertEquals("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi", url);
    }

    @Test
    void extractJmxConnectionUrl_multiLineYaml() {
        String yaml = "---\nstartDelaySeconds: 0\njmxUrl: \"service:jmx:rmi:///jndi/rmi://10.0.0.1:1099/jmxrmi\"\nssl: false\n";
        String url = KafkaJmxExporter.extractJmxConnectionUrl(yaml);
        assertEquals("service:jmx:rmi:///jndi/rmi://10.0.0.1:1099/jmxrmi", url);
    }

    // -------- parseArgs --------

    @Test
    void parseArgs_configOnly() {
        Namespace result = KafkaJmxExporter.parseArgs(new String[]{"--config", "/path/to/config.yaml"});
        assertNotNull(result);
        assertEquals("/path/to/config.yaml", result.getString("config"));
        assertEquals(KafkaJmxExporter.DEFAULT_HTTP_PORT, result.getInt("http_port"));
    }

    @Test
    void parseArgs_shortConfig() {
        Namespace result = KafkaJmxExporter.parseArgs(new String[]{"-c", "/path/to/config.yaml"});
        assertNotNull(result);
        assertEquals("/path/to/config.yaml", result.getString("config"));
    }

    @Test
    void parseArgs_configAndPort() {
        Namespace result = KafkaJmxExporter.parseArgs(
                new String[]{"--config", "/path/to/config.yaml", "--http-port", "9274"});
        assertNotNull(result);
        assertEquals("/path/to/config.yaml", result.getString("config"));
        assertEquals(9274, result.getInt("http_port"));
    }

    @Test
    void parseArgs_shortPortFlag() {
        Namespace result = KafkaJmxExporter.parseArgs(
                new String[]{"-c", "/path.yaml", "-p", "8080"});
        assertNotNull(result);
        assertEquals(8080, result.getInt("http_port"));
    }

    @Test
    void parseArgs_unknownArgReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(new String[]{"--unknown"}));
    }

    @Test
    void parseArgs_missingConfigReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(new String[]{}));
    }

    @Test
    void parseArgs_missingConfigValueReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(new String[]{"--config"}));
    }

    @Test
    void parseArgs_missingPortValueReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(new String[]{"-c", "config.yaml", "--http-port"}));
    }

    @Test
    void parseArgs_invalidPortReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(new String[]{"-c", "config.yaml", "--http-port", "notanumber"}));
    }

    @Test
    void parseArgs_defaultPort() {
        Namespace result = KafkaJmxExporter.parseArgs(new String[]{"-c", "config.yaml"});
        assertNotNull(result);
        assertEquals(KafkaJmxExporter.DEFAULT_HTTP_PORT, result.getInt("http_port"));
    }

    @Test
    void parseArgs_defaultFallbackPort() {
        Namespace result = KafkaJmxExporter.parseArgs(new String[]{"-c", "config.yaml"});
        assertNotNull(result);
        assertEquals(KafkaJmxExporter.DEFAULT_FALLBACK_HTTP_PORT, result.getInt("fallback_http_port"));
    }

    @Test
    void parseArgs_customFallbackPort() {
        Namespace result = KafkaJmxExporter.parseArgs(
                new String[]{"--config", "/path/to/config.yaml", "--fallback-http-port", "9275"});
        assertNotNull(result);
        assertEquals(KafkaJmxExporter.DEFAULT_HTTP_PORT, result.getInt("http_port"));
        assertEquals(9275, result.getInt("fallback_http_port"));
    }

    @Test
    void parseArgs_customBothPorts() {
        Namespace result = KafkaJmxExporter.parseArgs(
                new String[]{"-c", "/path.yaml", "-p", "8080", "-f", "8081"});
        assertNotNull(result);
        assertEquals(8080, result.getInt("http_port"));
        assertEquals(8081, result.getInt("fallback_http_port"));
    }

    @Test
    void parseArgs_invalidFallbackPortReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(
                new String[]{"-c", "config.yaml", "--fallback-http-port", "notanumber"}));
    }

    @Test
    void parseArgs_missingFallbackPortValueReturnsNull() {
        assertNull(KafkaJmxExporter.parseArgs(new String[]{"-c", "config.yaml", "--fallback-http-port"}));
    }
}
