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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.Objects;

public class RemoteReadContext {
    private final boolean blockPrefetchEnabled;
    private final boolean hedgedReadsEnabled;
    private final OffsetAndEpoch nextSegmentOffsetAndEpoch;
    private final boolean segmentPrefetchEnabled;

    private RemoteReadContext(boolean blockPrefetchEnabled,
                              boolean hedgedReadsEnabled,
                              OffsetAndEpoch nextSegmentOffsetAndEpoch,
                              boolean segmentPrefetchEnabled) {
        this.blockPrefetchEnabled = blockPrefetchEnabled;
        this.hedgedReadsEnabled = hedgedReadsEnabled;
        this.nextSegmentOffsetAndEpoch = nextSegmentOffsetAndEpoch;
        this.segmentPrefetchEnabled = segmentPrefetchEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean blockPrefetchEnabled = false;
        private boolean hedgedReadsEnabled = false;
        private OffsetAndEpoch nextSegmentOffsetAndEpoch = null;
        private boolean segmentPrefetchEnabled = false;

        private Builder() {
        }

        public Builder withBlockPrefetchEnabled(boolean blockPrefetchEnabled) {
            this.blockPrefetchEnabled = blockPrefetchEnabled;
            return this;
        }

        public Builder withHedgedReadsEnabled(boolean hedgedReadsEnabled) {
            this.hedgedReadsEnabled = hedgedReadsEnabled;
            return this;
        }

        public Builder withNextSegmentOffsetAndEpoch(OffsetAndEpoch nextSegmentOffsetAndEpoch) {
            this.nextSegmentOffsetAndEpoch = nextSegmentOffsetAndEpoch;
            return this;
        }

        public Builder withSegmentPrefetchEnabled(boolean segmentPrefetchEnabled) {
            this.segmentPrefetchEnabled = segmentPrefetchEnabled;
            return this;
        }

        public RemoteReadContext build() {
            return new RemoteReadContext(blockPrefetchEnabled, hedgedReadsEnabled, 
                                       nextSegmentOffsetAndEpoch, segmentPrefetchEnabled);
        }
    }

    public boolean isBlockPrefetchEnabled() {
        return blockPrefetchEnabled;
    }

    public boolean isHedgedReadsEnabled() {
        return hedgedReadsEnabled;
    }

    public boolean isSegmentPrefetchEnabled() {
        return segmentPrefetchEnabled;
    }

    public OffsetAndEpoch getNextSegmentOffsetAndEpoch() {
        return nextSegmentOffsetAndEpoch;
    }

    @Override
    public String toString() {
        return "RemoteReadContext{" +
                "prefetchEnabled=" + blockPrefetchEnabled +
                ", hedgedReadsEnabled=" + hedgedReadsEnabled +
                ", nextSegmentOffsetAndEpoch=" + nextSegmentOffsetAndEpoch +
                ", segmentPrefetchEnabled=" + segmentPrefetchEnabled +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteReadContext that = (RemoteReadContext) o;
        return blockPrefetchEnabled == that.blockPrefetchEnabled && hedgedReadsEnabled == that.hedgedReadsEnabled
                && Objects.equals(nextSegmentOffsetAndEpoch, that.nextSegmentOffsetAndEpoch)
                && segmentPrefetchEnabled == that.segmentPrefetchEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(blockPrefetchEnabled, hedgedReadsEnabled, nextSegmentOffsetAndEpoch, segmentPrefetchEnabled);
    }
}