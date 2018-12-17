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
package org.apache.kafka.common.security.token.delegation;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class InmemoryDelegationTokenStorageManager implements DelegationTokenStorageManager {

    private ConcurrentHashMap<String, DelegationToken> tokens = new ConcurrentHashMap<>();

    @Override
    public void init() {

    }

    @Override
    public void create(DelegationToken delegationToken) {
        Objects.requireNonNull(delegationToken, "delegationToken can not be null");
        tokens.put(delegationToken.tokenInfo().tokenId(), delegationToken);
    }

    @Override
    public void update(DelegationToken delegationToken) {
        Objects.requireNonNull(delegationToken, "delegationToken can not be null");
        tokens.put(delegationToken.tokenInfo().tokenId(), delegationToken);
    }

    @Override
    public DelegationToken fetch(String tokenId) {
        Objects.requireNonNull(tokenId, "tokenId can not be null");
        return tokens.get(tokenId);
    }

    @Override
    public boolean remove(String tokenId) {
        Objects.requireNonNull(tokenId, "tokenId can not be null");
        return tokens.remove(tokenId) != null;
    }

    @Override
    public void close() {

    }
}
