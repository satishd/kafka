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


package org.apache.kafka.message;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StructSpecTest {

    @Test
    public void shouldThrowErrorOnDuplicateTagId() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 0));
        fields.add(createFieldSpec("b", 0));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> new StructSpec("test", "0+", Versions.NONE_STRING, fields));
        assertTrue(ex.getMessage().contains("has a duplicate tag ID"));
    }

    @Test
    public void shouldThrowErrorWhenTagIdDoesNotStartWithZeroOr10K() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 1));
        fields.add(createFieldSpec("b", 2));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> new StructSpec("test", "0+", Versions.NONE_STRING, fields));
        assertTrue(ex.getMessage().contains("Make use of tag 0 or 10000 before using any higher tag IDs"));
    }

    @Test
    public void shouldThrowErrorWhenTagIdDoesNotStartWithZeroOr10K_1() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 10001));
        fields.add(createFieldSpec("b", 10002));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> new StructSpec("test", "0+", Versions.NONE_STRING, fields));
        assertTrue(ex.getMessage().contains("Make use of tag 0 or 10000 before using any higher tag IDs"));
    }

    @Test
    public void shouldThrowErrorWhenTagIdAreNotContiguous() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 0));
        fields.add(createFieldSpec("b", 1));
        fields.add(createFieldSpec("c", 3));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> new StructSpec("test", "0+", Versions.NONE_STRING, fields));
        assertTrue(ex.getMessage().contains("Make use of tag 2 or 10000 before using any higher tag IDs"));
    }

    @Test
    public void shouldThrowErrorWhenTagIdAreNotContiguous_1() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 0));
        fields.add(createFieldSpec("b", 1));
        fields.add(createFieldSpec("c", 10001));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> new StructSpec("test", "0+", Versions.NONE_STRING, fields));
        assertTrue(ex.getMessage().contains("Make use of tag 2 or 10000 before using any higher tag IDs"));
    }

    @Test
    public void shouldThrowErrorWhenTagIdAreNotContiguous_2() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 0));
        fields.add(createFieldSpec("b", 1));
        fields.add(createFieldSpec("c", 10000));
        fields.add(createFieldSpec("d", 10002));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> new StructSpec("test", "0+", Versions.NONE_STRING, fields));
        assertTrue(ex.getMessage().contains("Make use of tag 2 or 10001 before using any higher tag IDs"));
    }

    @Test
    public void shouldNotThrowErrorWhenTagIdStartsWithZeroOr10KAndContiguous() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 0));
        fields.add(createFieldSpec("b", 1));
        new StructSpec("test", "0+", Versions.NONE_STRING, fields);
    }

    @Test
    public void shouldNotThrowErrorWhenTagIdStartsWithZeroOr10KAndContiguous_1() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 10000));
        fields.add(createFieldSpec("b", 10001));
        new StructSpec("test", "0+", Versions.NONE_STRING, fields);
    }

    @Test
    public void shouldNotThrowErrorWhenTagIdStartsWithZeroOr10KAndContiguous_2() {
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(createFieldSpec("a", 0));
        fields.add(createFieldSpec("b", 1));
        fields.add(createFieldSpec("c", 10000));
        fields.add(createFieldSpec("d", 10001));
        fields.add(createFieldSpec("e", 10002));
        new StructSpec("test", "0+", Versions.NONE_STRING, fields);
    }

    private FieldSpec createFieldSpec(String name, int tag) {
        // Create a new instance of FieldSpec
        return new FieldSpec(name, "0+", null, "string", false, "0+",
                null, false, null, null, "0+", "0+",
                tag, false);

    }
}
