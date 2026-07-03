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
package org.apache.kafka.lake.internal;

/**
 * Small helpers for instantiating pluggable implementations (RSM, schema client) by class name,
 * shared so the reflective boilerplate lives in one place.
 */
public final class Plugins {

    private Plugins() {
    }

    /**
     * Load {@code className} from {@code loader} and instantiate it via its public no-arg constructor.
     *
     * @param loader    class loader to resolve the class from.
     * @param className fully qualified class name.
     * @param type      expected supertype; the instance is cast to it.
     * @return the new instance.
     * @throws IllegalStateException if the class cannot be loaded or instantiated.
     */
    public static <T> T newInstance(ClassLoader loader, String className, Class<T> type) {
        try {
            return type.cast(loader.loadClass(className).getDeclaredConstructor().newInstance());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to instantiate " + type.getSimpleName() + ": " + className, e);
        }
    }
}
