/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink.source;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link OceanBaseEnumeratorStateSerializer}. */
public class OceanBaseEnumeratorStateSerializerTest {

    @Test
    public void testStateRoundTripWithInFlightAndPendingSplits() throws IOException {
        OceanBaseSplit inFlight =
                new OceanBaseSplit("1", "test_schema", "test_table", "id", 10L, 20L);
        OceanBaseSplit pending =
                new OceanBaseSplit("2", "test_schema", "test_table", "id", 20L, 30L);

        OceanBaseEnumeratorState original =
                new OceanBaseEnumeratorState(Arrays.asList(inFlight), Arrays.asList(pending));

        OceanBaseEnumeratorStateSerializer serializer = new OceanBaseEnumeratorStateSerializer();
        byte[] bytes = serializer.serialize(original);
        OceanBaseEnumeratorState restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(1, restored.getInFlightSplits().size());
        assertEquals(1, restored.getPendingSplits().size());
        assertSplitEquals(inFlight, restored.getInFlightSplits().get(0));
        assertSplitEquals(pending, restored.getPendingSplits().get(0));
    }

    @Test
    public void testStateRoundTripWithNullLists() throws IOException {
        OceanBaseEnumeratorState original =
                new OceanBaseEnumeratorState(new ArrayList<>(), new ArrayList<>());

        OceanBaseEnumeratorStateSerializer serializer = new OceanBaseEnumeratorStateSerializer();
        OceanBaseEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

        assertEquals(0, restored.getInFlightSplits().size());
        assertEquals(0, restored.getPendingSplits().size());
    }

    private void assertSplitEquals(OceanBaseSplit expected, OceanBaseSplit actual) {
        assertEquals(expected.splitId(), actual.splitId());
        assertEquals(expected.getSchemaName(), actual.getSchemaName());
        assertEquals(expected.getTableName(), actual.getTableName());
        assertEquals(expected.getSplitColumn(), actual.getSplitColumn());
        assertEquals(expected.getSplitStart(), actual.getSplitStart());
        assertEquals(expected.getSplitEnd(), actual.getSplitEnd());
        assertEquals(expected.isFirstSplit(), actual.isFirstSplit());
        assertEquals(expected.isLastSplit(), actual.isLastSplit());
    }
}
