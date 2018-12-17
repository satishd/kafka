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

import java.util.Arrays;

public class CreateDelegationTokenResult {

    private final Long issueTimestamp;
    private final Long expiryTimestamp;
    private final Long maxTimestamp;
    private final String tokenId;
    private final byte[] hmac;
    private final Errors error;

    public CreateDelegationTokenResult(Long issueTimestamp,
                                       Long expiryTimestamp,
                                       Long maxTimestamp,
                                       String tokenId,
                                       byte[] hmac,
                                       Errors error) {
        this.issueTimestamp = issueTimestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.tokenId = tokenId;
        this.hmac = hmac;
        this.error = error;
    }

    public Long issueTimestamp() {
        return issueTimestamp;
    }

    public Long expiryTimestamp() {
        return expiryTimestamp;
    }

    public Long maxTimestamp() {
        return maxTimestamp;
    }

    public String tokenId() {
        return tokenId;
    }

    public byte[] hmac() {
        return hmac;
    }

    public Errors error() {
        return error;
    }

    @Override
    public String toString() {
        return "CreateDelegationTokenResult{" +
               "issueTimestamp=" + issueTimestamp +
               ", expiryTimestamp=" + expiryTimestamp +
               ", maxTimestamp=" + maxTimestamp +
               ", tokenId='" + tokenId + '\'' +
               ", hmac=" + Arrays.toString(hmac) +
               ", error=" + error +
               '}';
    }
}
