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
package org.apache.kafka.common;

import java.util.Objects;
import java.util.UUID;

/**
 *
 */
public class TopicIdPartition {

    private static final UUID DEFAULT = new UUID(1, 1);

    private final UUID topicId;
    private final TopicPartition topicPartition;

    /**
     * It uses {@link #DEFAULT as the unique id for this partition}.
     *
     * This constructor will be removed once topic id change is introduced as part of KIP-516.
     *
     * @param topicPartition
     */
    public TopicIdPartition(TopicPartition topicPartition) {
        this(DEFAULT, topicPartition);
    }

    public TopicIdPartition(UUID topicId, TopicPartition topicPartition) {
        Objects.requireNonNull(topicId, "topicId can not be null");
        Objects.requireNonNull(topicPartition, "topicPartition can not be null");
        this.topicId = topicId;
        this.topicPartition = topicPartition;
    }

    public UUID topicId() {
        return topicId;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicIdPartition that = (TopicIdPartition) o;
        return Objects.equals(topicId, that.topicId) &&
                Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, topicPartition);
    }

    @Override
    public String toString() {
        return "TopicIdPartition{" +
                "id=" + topicId +
                ", topicPartition=" + topicPartition +
                '}';
    }
}
