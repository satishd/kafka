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

import java.util.Objects;

public class SegmentHeaderHolder {
    private final LogSegmentDataHeader header;
    private final long fileLength;

    public SegmentHeaderHolder(LogSegmentDataHeader header, long fileLength) {
        this.header = header;
        this.fileLength = fileLength;
    }

    public LogSegmentDataHeader header() {
        return header;
    }

    public long fileLength() {
        return fileLength;
    }

    @Override
    public String toString() {
        return "SegmentHeaderHolder{" +
                "header=" + header +
                ", fileLength=" + fileLength +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentHeaderHolder that = (SegmentHeaderHolder) o;
        return fileLength == that.fileLength && Objects.equals(header, that.header);
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, fileLength);
    }
}
