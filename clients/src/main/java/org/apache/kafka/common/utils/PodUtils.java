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
package org.apache.kafka.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to support pod isolatione
 */
public class PodUtils {
    private static final Pattern POD_RACK_REGEX = Pattern.compile("^(.*)::(.*)$");
    private static final String POD_RACK_FORMAT = "%s::%s";
    private static final int GROUP_INDEX_POD = 1;
    private static final int GROUP_INDEX_RACK = 2;

    /**
     * Encodes pod and rack into podRack string in the format of [POD]::[RACK]
     * if pod is empty returns rack only
     *
     * @param rack the rack
     * @param pod  the pod
     * @return the string
     */
    public static String toPodAndRack(String pod, String rack) {
        if (pod == null || pod.isEmpty()) {
            return rack;
        }
        return String.format(POD_RACK_FORMAT, pod, rack == null ? "" : rack);
    }

    /**
     * Decodes pod from podRack String, if the input is not podRack encoded string, then returns null.
     *
     * @param podAndRack the `Pod::Rack` value
     * @return the extracted pod information
     */
    public static String podOf(String podAndRack) {
        if (podAndRack != null) {
            Matcher matcher = POD_RACK_REGEX.matcher(podAndRack);
            if (matcher.matches()) {
                return matcher.group(GROUP_INDEX_POD);
            }
        }
        return null;
    }

    /**
     * Decodes rack from podAndRack string, if the input is not podAndRack encoded, then returns the input string.
     *
     * @param podAndRack the `Pod::Rack` value
     * @return the extracted rack information
     */
    public static String rackOf(String podAndRack) {
        if (podAndRack != null) {
            Matcher matcher = POD_RACK_REGEX.matcher(podAndRack);
            if (matcher.matches()) {
                return matcher.group(GROUP_INDEX_RACK);
            }
        }
        return podAndRack;
    }
}
