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

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;

public class CanaryConfigs {

    public static final String CANARY_PARTITION_PERCENTAGE = "canary.partition.percentage";
    public static final double CANARY_PARTITION_PERCENTAGE_DEFAULT = 0.0d;
    public static final String CANARY_PARTITION_PERCENTAGE_DOC = "Percentage of partitions that should be canary partitions; 0.0 means disabled";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CANARY_PARTITION_PERCENTAGE, DOUBLE, CANARY_PARTITION_PERCENTAGE_DEFAULT, ConfigDef.Range.between(0.0d, 1.0d), MEDIUM, CANARY_PARTITION_PERCENTAGE_DOC);
}
