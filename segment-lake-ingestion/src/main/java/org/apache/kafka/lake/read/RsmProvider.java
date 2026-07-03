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
package org.apache.kafka.lake.read;

import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.lake.internal.Plugins;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.IOException;
import java.util.Map;

/**
 * Creates and owns a configured {@link RemoteStorageManager}.
 *
 * <p>The physical location of a segment is not computed here: it is resolved inside the RSM from the
 * segment's {@code customMetadata} (the remote bucket). This provider only instantiates the RSM the
 * same way the broker's {@code RemoteLogManager} does — by class name, optionally from a child-first
 * class loader over a configured classpath — and configures it.
 */
public class RsmProvider implements AutoCloseable {

    private final RemoteStorageManager remoteStorageManager;

    public RsmProvider(ConverterConfig config) {
        this(create(config.rsmClassName(), config.rsmClassPath(), config.rsmConfigs()));
    }

    // Visible for testing.
    RsmProvider(RemoteStorageManager remoteStorageManager) {
        this.remoteStorageManager = remoteStorageManager;
    }

    public RemoteStorageManager storageManager() {
        return remoteStorageManager;
    }

    private static RemoteStorageManager create(String className, String classPath, Map<String, Object> configs) {
        if (className == null || className.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    ConverterConfig.RSM_CLASS_NAME_CONFIG + " must be set to instantiate a RemoteStorageManager");
        }
        final RemoteStorageManager rsm;
        if (classPath != null && !classPath.trim().isEmpty()) {
            ChildFirstClassLoader classLoader = new ChildFirstClassLoader(classPath, RsmProvider.class.getClassLoader());
            RemoteStorageManager delegate = Plugins.newInstance(classLoader, className, RemoteStorageManager.class);
            rsm = new ClassLoaderAwareRemoteStorageManager(delegate, classLoader);
        } else {
            rsm = Plugins.newInstance(RsmProvider.class.getClassLoader(), className, RemoteStorageManager.class);
        }
        rsm.configure(configs);
        return rsm;
    }

    @Override
    public void close() throws IOException {
        remoteStorageManager.close();
    }
}
