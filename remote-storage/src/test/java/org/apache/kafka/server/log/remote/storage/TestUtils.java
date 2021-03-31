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
package org.apache.kafka.server.log.remote.storage;

import org.junit.jupiter.api.Assertions;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class TestUtils {

    /**
     * Returns true if both iterators have same elements in the same order.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param <T>       type of element in the iterators.
     * @return
     */
    public static <T> boolean sameElementsWithOrder(Iterator<T> iterator1,
                                                    Iterator<T> iterator2) {
        while (iterator1.hasNext()) {
            if (!iterator2.hasNext()) {
                return false;
            }

            Object elem1 = iterator1.next();
            Object elem2 = iterator2.next();
            if (!Objects.equals(elem1, elem2)) {
                return false;
            }
        }

        return !iterator2.hasNext();
    }

    /**
     * Returns true if both the iterators have same set of elements irrespective of order and duplicates.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param <T>       type of element in the iterators.
     * @return
     */
    public static <T> boolean sameElementsWithoutOrder(Iterator<T> iterator1,
                                                       Iterator<T> iterator2) {
        // check both the iterators have the same set of elements irrespective of order and duplicates.
        Set<T> allSegmentsSet = new HashSet<>();
        iterator1.forEachRemaining(allSegmentsSet::add);
        Set<T> expectedSegmentsSet = new HashSet<>();
        iterator2.forEachRemaining(expectedSegmentsSet::add);

        return allSegmentsSet.equals(expectedSegmentsSet);
    }
}