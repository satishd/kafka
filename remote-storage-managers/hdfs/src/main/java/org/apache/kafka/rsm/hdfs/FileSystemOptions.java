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

public class FileSystemOptions {
    private final String bucket;
    private final boolean hedgedReadsEnabled;
    private final boolean readAheadEnabled;

    /**
     * Creates a new FileSystemOptions with the specified bucket and default values for other options.
     *
     * @param bucket the bucket URI
     */
    public FileSystemOptions(String bucket) {
        this(bucket, false, false);
    }

    /**
     * Creates a new FileSystemOptions with the specified bucket and hedgedReadsEnabled option.
     *
     * @param bucket the bucket URI
     * @param hedgedReadsEnabled whether hedged reads are enabled
     */
    public FileSystemOptions(String bucket, boolean hedgedReadsEnabled) {
        this(bucket, hedgedReadsEnabled, false);
    }

    /**
     * Creates a new FileSystemOptions with all options specified.
     *
     * @param bucket the bucket URI
     * @param hedgedReadsEnabled whether hedged reads are enabled
     * @param readAheadEnabled whether read ahead is enabled
     */
    public FileSystemOptions(String bucket, boolean hedgedReadsEnabled, boolean readAheadEnabled) {
        this.bucket = bucket;
        this.hedgedReadsEnabled = hedgedReadsEnabled;
        this.readAheadEnabled = readAheadEnabled;
    }

    public String bucket() {
        return bucket;
    }

    public boolean hedgedReadsEnabled() {
        return hedgedReadsEnabled;
    }

    public boolean readAheadEnabled() {
        return readAheadEnabled;
    }
}
