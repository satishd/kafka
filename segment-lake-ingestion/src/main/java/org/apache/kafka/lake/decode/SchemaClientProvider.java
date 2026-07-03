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
package org.apache.kafka.lake.decode;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.lake.config.ConverterConfig;

import java.util.Collections;

/**
 * Instantiates the configured {@link SchemaClient} by class name, mirroring the
 * {@code RsmProvider} pattern: same JVM class loader (no child-first classpath, unlike the RSM,
 * since the schema client is expected to be an in-process HTTP client with no conflicting
 * dependencies), and configured via {@link Configurable#configure} if it implements it.
 */
public final class SchemaClientProvider {

    private SchemaClientProvider() {
    }

    public static SchemaClient create(ConverterConfig config) {
        String className = config.schemaClientClassName();
        if (className == null || className.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    ConverterConfig.SCHEMA_CLIENT_CLASS_NAME_CONFIG + " must be set to instantiate a SchemaClient");
        }
        SchemaClient client;
        try {
            client = (SchemaClient) Class.forName(className)
                    .getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate SchemaClient: " + className, e);
        }
        if (client instanceof Configurable) {
            ((Configurable) client).configure(Collections.unmodifiableMap(config.schemaClientConfigs()));
        }
        return client;
    }
}
