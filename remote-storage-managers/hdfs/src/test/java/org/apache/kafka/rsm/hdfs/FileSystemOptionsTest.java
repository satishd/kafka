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
package org.apache.kafka.rsm.hdfs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileSystemOptionsTest {

    private static final String TEST_BUCKET = "hdfs://localhost:9000";

    @Test
    public void testConstructorWithBucketOnly() {
        FileSystemOptions options = new FileSystemOptions(TEST_BUCKET);
        
        assertEquals(TEST_BUCKET, options.bucket());
        assertFalse(options.hedgedReadsEnabled());
        assertFalse(options.readAheadEnabled());
    }

    @Test
    public void testConstructorWithBucketAndHedgedReads() {
        FileSystemOptions options = new FileSystemOptions(TEST_BUCKET, true);
        
        assertEquals(TEST_BUCKET, options.bucket());
        assertTrue(options.hedgedReadsEnabled());
        assertFalse(options.readAheadEnabled());
    }

    @Test
    public void testConstructorWithAllParameters() {
        FileSystemOptions options = new FileSystemOptions(TEST_BUCKET, true, true);
        
        assertEquals(TEST_BUCKET, options.bucket());
        assertTrue(options.hedgedReadsEnabled());
        assertTrue(options.readAheadEnabled());
    }

    @Test
    public void testConstructorWithHedgedReadsDisabledAndReadAheadEnabled() {
        FileSystemOptions options = new FileSystemOptions(TEST_BUCKET, false, true);
        
        assertEquals(TEST_BUCKET, options.bucket());
        assertFalse(options.hedgedReadsEnabled());
        assertTrue(options.readAheadEnabled());
    }
}