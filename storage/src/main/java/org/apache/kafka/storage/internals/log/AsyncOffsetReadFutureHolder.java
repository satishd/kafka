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
package org.apache.kafka.storage.internals.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A remote log offset read task future holder. It contains two futures:
 * 1. JobFuture - Use this future to cancel the running job.
 * 2. TaskFuture - Use this future to get the result of the job/computation.
 */
public class AsyncOffsetReadFutureHolder<T> {
    private final Future<Void> jobFuture;
    private final CompletableFuture<T> taskFuture;

    public AsyncOffsetReadFutureHolder(Future<Void> jobFuture, CompletableFuture<T> taskFuture) {
        this.jobFuture = jobFuture;
        this.taskFuture = taskFuture;
    }

    public Future<Void> jobFuture() {
        return jobFuture;
    }

    public CompletableFuture<T> taskFuture() {
        return taskFuture;
    }

    @Override
    public String toString() {
        return "AsyncOffsetReadFutureHolder{" +
                "jobFuture=" + jobFuture +
                ", taskFuture=" + taskFuture +
                '}';
    }
}