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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordFieldExtractorTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"maybe\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    @Test
    public void returnsFieldValueAsString() {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", "order-1");
        assertEquals("order-1", RecordFieldExtractor.forField("id").apply(record));
    }

    @Test
    public void throwsWhenFieldIsNull() {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", "order-1");
        assertThrows(IllegalStateException.class, () -> RecordFieldExtractor.forField("maybe").apply(record));
    }
}
