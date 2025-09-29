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

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public class SafeInputStream extends InputStream {
    private final InputStream delegate;
    private final Consumer<IOException> errorHandler;

    public SafeInputStream(InputStream delegate, Consumer<IOException> errorHandler) {
        this.delegate = delegate;
        this.errorHandler = errorHandler;
    }

    public InputStream delegate() {
        return delegate;
    }

    @Override
    public int read() throws IOException {
        return executeWithErrorHandling(delegate::read);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return executeWithErrorHandling(() -> delegate.read(b));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return executeWithErrorHandling(() -> delegate.read(b, off, len));
    }

    @Override
    public int available() throws IOException {
        return executeWithErrorHandling(delegate::available);
    }

    @Override
    public long skip(long n) throws IOException {
        return executeWithErrorHandling(() -> delegate.skip(n));
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        executeWithErrorHandling(() -> {
            delegate.reset();
            return null;
        });
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public void close() throws IOException {
        executeWithErrorHandling(() -> {
            delegate.close();
            return null;
        });
    }

    private <T> T executeWithErrorHandling(IOSupplier<T> operation) throws IOException {
        try {
            return operation.get();
        } catch (IOException e) {
            errorHandler.accept(e);
            throw e;
        }
    }

    @FunctionalInterface
    private interface IOSupplier<T> {
        T get() throws IOException;
    }

}