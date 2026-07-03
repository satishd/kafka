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

import org.apache.avro.Schema;

/**
 * Resolves the writer {@link Schema} Heatpipe used to encode a topic's records at a given schema
 * version.
 *
 * <p>Production wiring is expected to be backed by Uber's Schema Service via {@code heatpipe4j};
 * that implementation is not included here pending resolution of the library's Maven coordinates
 * and API (see the design doc's open risks). This interface lets {@link HeatpipeAvroDecoder} be
 * fully implemented and unit-tested now, with the real client swapped in later without touching
 * decode logic.
 */
public interface SchemaClient {

    /**
     * @param topic         the topic the record belongs to.
     * @param schemaVersion the schema version read from the {@link HeatpipeHeader}.
     * @return the writer schema for that (topic, schemaVersion).
     * @throws SchemaFetchException if the schema could not be resolved.
     */
    Schema schemaFor(String topic, int schemaVersion) throws SchemaFetchException;
}
