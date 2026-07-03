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
package org.apache.kafka.lake.write;

import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

/**
 * Derives a string value (Hudi record key or partition path) from a named field of a decoded
 * {@link GenericRecord}. Keeping this out of the write client itself lets {@link HudiSegmentWriter}
 * stay agnostic of any particular topic's schema.
 */
public final class RecordFieldExtractor {

    private RecordFieldExtractor() {
    }

    /**
     * @param fieldName the decoded-record field to read.
     * @return a function returning {@code record.get(fieldName).toString()}, throwing
     *         {@link IllegalStateException} if the field is absent/null.
     */
    public static Function<GenericRecord, String> forField(String fieldName) {
        return record -> {
            Object value = record.get(fieldName);
            if (value == null) {
                throw new IllegalStateException("Decoded record is missing required field '" + fieldName + "'");
            }
            return value.toString();
        };
    }
}
