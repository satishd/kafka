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
