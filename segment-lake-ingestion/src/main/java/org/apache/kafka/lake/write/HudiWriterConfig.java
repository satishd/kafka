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

/**
 * Table-level settings for {@link HudiSegmentWriter}. The table base path is intentionally an
 * opaque string: whether it uses {@code cfs://} or {@code oci://} is an OCS/CloudLake decision
 * outside this module's scope (see the design doc's open risks) and is fully determined by config.
 */
public final class HudiWriterConfig {

    private final String tableBasePath;
    private final String tableName;
    private final String recordKeyField;
    private final String partitionPathField;

    public HudiWriterConfig(String tableBasePath, String tableName,
                             String recordKeyField, String partitionPathField) {
        this.tableBasePath = tableBasePath;
        this.tableName = tableName;
        this.recordKeyField = recordKeyField;
        this.partitionPathField = partitionPathField;
    }

    public String tableBasePath() {
        return tableBasePath;
    }

    public String tableName() {
        return tableName;
    }

    public String recordKeyField() {
        return recordKeyField;
    }

    public String partitionPathField() {
        return partitionPathField;
    }
}
