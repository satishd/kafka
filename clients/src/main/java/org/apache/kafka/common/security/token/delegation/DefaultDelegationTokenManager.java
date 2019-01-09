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

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DefaultDelegationTokenManager implements IDelegationTokenManager {
    private static Logger log = LoggerFactory.getLogger(DefaultDelegationTokenManager.class);

    private static final String MASTER_KEY = "masterKey";
    private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA512";

    private static final SecretKey SECRET_KEY;

    static {
        SECRET_KEY = new SecretKeySpec(MASTER_KEY.getBytes(StandardCharsets.UTF_8), DEFAULT_HMAC_ALGORITHM);
    }

    private final Time time;

    private DelegationTokenCache tokenCache;
    private long tokenMaxLifeMs;
    private long defaultTokenRenewTime;
    private DelegationTokenStorageManager delegationTokenStorageManager;

    public DefaultDelegationTokenManager(Time time, DelegationTokenCache tokenCache) {
        this.time = time;
        this.tokenCache = tokenCache;
    }

    byte[] createHmac(String tokenId) {
        Mac mac = null;
        try {
            mac = Mac.getInstance(DEFAULT_HMAC_ALGORITHM);
            mac.init(SECRET_KEY);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid key to HMAC computation", e);
        }
        return mac.doFinal(tokenId.getBytes(StandardCharsets.UTF_8));
    }

    String encodedHmac(String tokenId) {
        byte[] hmac = createHmac(tokenId);
        return Base64.getEncoder().encodeToString(hmac);
    }

    public DefaultDelegationTokenManager(Time time) {
        this(time, new DelegationTokenCache(ScramMechanism.mechanismNames()));
    }

    @Override
    public void init(DelegationTokenManagerConfig config) {
        tokenMaxLifeMs = config.delegationTokenMaxLifeMs();
        defaultTokenRenewTime = config.delegationTokenExpiryTimeMs();
        try {
            Class<?> klass = Class.forName(config.storageManagerClassName(),
                                           true,
                                           Utils.getContextOrKafkaClassLoader());
            delegationTokenStorageManager = (DelegationTokenStorageManager) klass.getConstructor().newInstance();
            delegationTokenStorageManager.init();

            tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(KafkaPrincipal owner, List<KafkaPrincipal> renewers, Long maxLifeTimeMs) {
        String tokenId = generateUuidAsBase64();

        long issueTimeStamp = time.milliseconds();
        long maxLifeTime = (maxLifeTimeMs <= 0) ? tokenMaxLifeMs : Math.min(maxLifeTimeMs, tokenMaxLifeMs);
        long maxLifeTimeStamp = issueTimeStamp + maxLifeTime;
        long expiryTimeStamp = Math.min(maxLifeTimeStamp, issueTimeStamp + defaultTokenRenewTime);

        TokenInformation tokenInfo = new TokenInformation(tokenId, owner, renewers, issueTimeStamp, maxLifeTimeStamp, expiryTimeStamp);

        byte[] hmac = createHmac(tokenId);
        DelegationToken delegationToken = new DelegationToken(tokenInfo, hmac);
        delegationTokenStorageManager.create(delegationToken);
        tokenCache.updateCache(delegationToken);

        return new CreateDelegationTokenResult(issueTimeStamp, expiryTimeStamp, maxLifeTimeStamp, tokenId, hmac, Errors.NONE);
    }

    public DelegationTokenCache tokenCache() {
        return tokenCache;
    }

    private String generateUuidAsBase64() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return Base64.getEncoder().encodeToString(byteBuffer.array());
    }

    @Override
    public TokenOperationResult renewDelegationToken(KafkaPrincipal principal, ByteBuffer hmac, Long renewLifeTimeMs) {
        Optional<DelegationToken> delegationToken = delegationTokenForHmac(hmac);
        TokenOperationResult result;
        if (delegationToken.isPresent()) {
            long now = time.milliseconds();
            TokenInformation tokenInfo = delegationToken.get().tokenInfo();

            if (!allowedToRenew(principal, tokenInfo)) {
                result = new TokenOperationResult(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, -1L);
            } else if (tokenInfo.maxTimestamp() < now || tokenInfo.expiryTimestamp() < now) {
                result = new TokenOperationResult(Errors.DELEGATION_TOKEN_EXPIRED, -1L);
            } else {
                long renewLifeTime = renewLifeTimeMs < 0 ? defaultTokenRenewTime : renewLifeTimeMs;

                long renewTimeStamp = now + renewLifeTime;
                long expiryTimeStamp = Math.min(tokenInfo.maxTimestamp(), renewTimeStamp);
                tokenInfo.setExpiryTimestamp(expiryTimeStamp);

                updateToken(delegationToken.get());
                log.info("Delegation token renewed for token: {} for owner: {}", tokenInfo, tokenInfo.owner());
                result = new TokenOperationResult(Errors.NONE, expiryTimeStamp);
            }
        } else {
            result = new TokenOperationResult(Errors.DELEGATION_TOKEN_NOT_FOUND, -1L);
        }

        return result;
    }

    private boolean allowedToRenew(KafkaPrincipal principal, TokenInformation tokenInfo) {
        return principal.equals(tokenInfo.owner()) || tokenInfo.renewers().contains(principal);

    }

    private Optional<DelegationToken> delegationTokenForHmac(ByteBuffer hmac) {
        byte[] array = hmac.array();
        TokenInformation tokenInfo = tokenCache.tokenForHmac(Base64.getEncoder().encodeToString(array));
        return tokenInfo == null ? Optional.empty() : Optional.of(new DelegationToken(tokenInfo, array));
    }

    @Override
    public TokenOperationResult expireDelegationToken(KafkaPrincipal principal, ByteBuffer hmac, Long expireLifeTimeMs) {
        Optional<DelegationToken> delegationToken = delegationTokenForHmac(hmac);
        TokenOperationResult result;
        if (delegationToken.isPresent()) {
            TokenInformation tokenInfo = delegationToken.get().tokenInfo();
            long now = time.milliseconds();

            if (!allowedToRenew(principal, tokenInfo)) {
                result = new TokenOperationResult(Errors.DELEGATION_TOKEN_OWNER_MISMATCH, -1L);
            } else if (tokenInfo.maxTimestamp() < now || tokenInfo.expiryTimestamp() < now) {
                result = new TokenOperationResult(Errors.DELEGATION_TOKEN_EXPIRED, -1L);
            } else if (expireLifeTimeMs < 0) { //expire immediately
                removeToken(tokenInfo.tokenId());
                log.info("Token expired for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}");
                result = new TokenOperationResult(Errors.NONE, now);
            } else {
                //set expiry time stamp
                long expiryTimeStamp = Math.min(tokenInfo.maxTimestamp(), now + expireLifeTimeMs);
                tokenInfo.setExpiryTimestamp(expiryTimeStamp);

                updateToken(delegationToken.get());
                log.info("Updated expiry time for token: ${tokenInfo.tokenId} for owner: ${tokenInfo.owner}");
                result = new TokenOperationResult(Errors.NONE, expiryTimeStamp);
            }
        } else {
            result = new TokenOperationResult(Errors.DELEGATION_TOKEN_NOT_FOUND, -1L);
        }
        return result;
    }

    protected void updateToken(DelegationToken delegationToken) {
        delegationTokenStorageManager.update(delegationToken);
        tokenCache.updateCache(delegationToken);
    }

    protected void removeToken(String tokenId) {
        delegationTokenStorageManager.remove(tokenId);
        tokenCache.removeToken(tokenId);
    }

    @Override
    public void expireTokens() {
        for (TokenInformation tokenInfo : tokenCache.tokens()) {
            long now = time.milliseconds();
            if (tokenInfo.maxTimestamp() < now || tokenInfo.expiryTimestamp() < now) {
                log.info("Delegation token expired for token: [{}] for owner: [{}]", tokenInfo.tokenId(), tokenInfo.owner());
                removeToken(tokenInfo.tokenId());
            }
        }
    }

    @Override
    public Optional<DelegationToken> getDelegationToken(String tokenId) {
        DelegationToken delegationToken = null;
        TokenInformation tokenInformation = tokenCache.token(tokenId);
        if (tokenInformation != null) {
            delegationToken = buildDelegationToken(tokenInformation);
        }
        return Optional.ofNullable(delegationToken);
    }

    private DelegationToken buildDelegationToken(TokenInformation tokenInformation) {
        byte[] hmac = createHmac(tokenInformation.tokenId());
        return new DelegationToken(tokenInformation, hmac);
    }

    @Override
    public List<DelegationToken> getDelegationTokens(Predicate<TokenInformation> predicate) {
        return tokenCache.tokens().stream()
                         .map(this::buildDelegationToken)
                         .filter(x -> predicate.test(x.tokenInfo()))
                         .collect(Collectors.toList());
    }

    @Override
    public Optional<ScramCredential> credential(String mechanism, String tokenId) {
        return Optional.ofNullable(tokenCache.credential(mechanism, tokenId));
    }

    @Override
    public void shutdown() {
        Utils.closeQuietly(delegationTokenStorageManager, delegationTokenStorageManager.getClass().getName());
    }

    public void addToken(String tokenId, TokenInformation tokenInfo) {
        tokenCache.addToken(tokenId, tokenInfo);
    }
}
