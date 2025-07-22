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

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSDataFetcher implements DataFetcher {
    private final String hadoopBaseDir;
    private final FileSystemManager fileSystemManager;

    public HDFSDataFetcher(String hadoopBaseDir, FileSystemManager fileSystemManager) {
        this.hadoopBaseDir = hadoopBaseDir;
        this.fileSystemManager = fileSystemManager;
    }

    private FileSystem getFS(RemoteLogSegmentMetadata metadata) {
        String bucket = fileSystemManager.getBucket(metadata);
        FileSystemOptions options = new FileSystemOptions(bucket, true, true);
        return fileSystemManager.getFS(options);
    }

    public long fileLength(RemoteLogSegmentMetadata metadata) throws IOException {
        String filePath = getSegmentRemoteDir(metadata.remoteLogSegmentId());
        return getFS(metadata).getFileStatus(new Path(filePath)).getLen();
    }

    @Override
    public FSDataInputStream fetchSegmentData(RemoteLogSegmentMetadata metadata) throws IOException {
        RemoteLogSegmentId segmentId = metadata.remoteLogSegmentId();
        String filePath = getSegmentRemoteDir(segmentId);
        return getFS(metadata).open(new Path(filePath));
    }

    private String getSegmentRemoteDir(RemoteLogSegmentId remoteLogSegmentId) {
        return RSMUtils.getSegmentRemoteDir(hadoopBaseDir, remoteLogSegmentId);
    }
}
