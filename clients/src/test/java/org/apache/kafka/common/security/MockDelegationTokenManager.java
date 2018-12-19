package org.apache.kafka.common.security;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.CreateDelegationTokenResult;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.DelegationTokenConfig;
import org.apache.kafka.common.security.token.delegation.IDelegationTokenManager;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MockDelegationTokenManager implements IDelegationTokenManager {
    private static Logger log = LoggerFactory.getLogger(MockDelegationTokenManager.class);

    private static final String masterKey = "masterKey";
    private static final String DefaultHmacAlgorithm = "HmacSHA512";

    static SecretKey secretKey = null;

    static {
        secretKey = new SecretKeySpec(masterKey.getBytes(StandardCharsets.UTF_8), DefaultHmacAlgorithm);
    }

    private final Time time;

    private Map<String, DelegationToken> tokens = new ConcurrentHashMap<>();
    private DelegationTokenCache tokenCache;
    private long tokenMaxLifeMs;
    private long defaultTokenRenewTime;

    public MockDelegationTokenManager(Time time, DelegationTokenCache tokenCache) {
        this.time = time;
        this.tokenCache = tokenCache;
    }

    byte[] createHmac(String tokenId) {
        Mac mac = null;
        try {
            mac = Mac.getInstance(DefaultHmacAlgorithm);
            mac.init(secretKey);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid key to HMAC computation", e);
        }
        return mac.doFinal(tokenId.getBytes(StandardCharsets.UTF_8));
    }

    String encodedHmac(String tokenId) {
        byte[] hmac = createHmac(tokenId);
        return Base64.getEncoder().encodeToString(hmac);
    }

    public MockDelegationTokenManager(Time time) {
        this(time, new DelegationTokenCache(ScramMechanism.mechanismNames()));
    }

    @Override
    public void startup(DelegationTokenConfig config) {
        tokenMaxLifeMs = config.delegationTokenMaxLifeMs();
        defaultTokenRenewTime = config.delegationTokenExpiryTimeMs();
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
        DelegationToken token = new DelegationToken(tokenInfo, hmac);
        tokens.put(tokenId, token);
        tokenCache.updateCache(token);

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

    private void updateToken(DelegationToken delegationToken) {
        //todo
        tokens.put(delegationToken.tokenInfo().tokenId(), delegationToken);
//        tokenCache.updateCache(token, );
    }

    private void removeToken(String tokenId) {
        tokens.remove(tokenId);
        tokenCache.removeToken(tokenId);
    }

    @Override
    public void expireTokens() {
        for (DelegationToken value : tokens.values()) {
            long now = time.milliseconds();
            TokenInformation tokenInfo = value.tokenInfo();
            if (tokenInfo.maxTimestamp() < now || tokenInfo.expiryTimestamp() < now) {
                log.info("Delegation token expired for token: [{}] for owner: [{}]", tokenInfo.tokenId(), tokenInfo.owner());
                removeToken(tokenInfo.tokenId());
            }
        }
    }

    @Override
    public Optional<DelegationToken> getDelegationToken(String tokenId) {
        return Optional.ofNullable(tokens.get(tokenId));
    }

    @Override
    public List<DelegationToken> getDelegationTokens(Predicate<TokenInformation> predicate) {
        return tokens.values().stream().filter(x -> predicate.test(x.tokenInfo())).collect(Collectors.toList());
    }

    @Override
    public Optional<ScramCredential> credential(String mechanism, String tokenId) {
        return Optional.ofNullable(tokenCache.credential(mechanism, tokenId));
    }

    @Override
    public void shutdown() {

    }

    public void addToken(String tokenId, TokenInformation tokenInfo) {
        tokenCache.addToken(tokenId, tokenInfo);
    }
}
