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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

public interface IDelegationTokenManager {

    /**
     * Initialize any resources that are required for this instance.
     * @param config
     */
    void init(DelegationTokenManagerConfig config);

    /**
     * @param owner
     * @param renewers
     * @param maxLifeTimeMs
     * @return
     */
    CreateDelegationTokenResult createDelegationToken(KafkaPrincipal owner,
                                                      List<KafkaPrincipal> renewers,
                                                      Long maxLifeTimeMs);

    /**
     * @param principal
     * @param hmac
     * @param renewLifeTimeMs
     * @return
     */
    TokenOperationResult renewDelegationToken(KafkaPrincipal principal,
                                              ByteBuffer hmac,
                                              Long renewLifeTimeMs);

    /**
     * Expire the token associated with given hmac.
     *
     * @param principal
     * @param hmac
     * @param expireLifeTimeMs
     * @return
     */
    TokenOperationResult expireDelegationToken(KafkaPrincipal principal,
                                               ByteBuffer hmac,
                                               Long expireLifeTimeMs);

    /**
     * Expire all available tokens
     */
    void expireTokens();

    /**
     * Returns None if TokenInfo or DelegationToken does not exist for the given tokenId.
     *
     * @param tokenId
     * @return
     */
    Optional<DelegationToken> getDelegationToken(String tokenId);

    /**
     * @param predicate
     * @return
     */
    List<DelegationToken> getDelegationTokens(Predicate<TokenInformation> predicate);

    /**
     * @param mechanism
     * @param tokenId
     * @return
     */
    Optional<ScramCredential> credential(String mechanism, String tokenId);

    /**
     * Release any system resources associated with this instance.
     */
    void shutdown();

    class TokenOperationResult {
        private Errors error;
        private Long timestamp;

        public TokenOperationResult(Errors error, Long timestamp) {
            this.error = error;
            this.timestamp = timestamp;
        }

        public Errors getError() {
            return error;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TokenOperationResult that = (TokenOperationResult) o;
            return error == that.error &&
                   Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(error, timestamp);
        }

        @Override
        public String toString() {
            return "TokenManagerResponse{" +
                   "error=" + error +
                   ", timestamp=" + timestamp +
                   '}';
        }
    }
}
