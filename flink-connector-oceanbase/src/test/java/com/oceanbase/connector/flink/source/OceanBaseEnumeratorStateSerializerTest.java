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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertTrue(restored.isSplitDiscoveryFinished());
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
        assertTrue(restored.isSplitDiscoveryFinished());
    }

    @Test
    public void testStateRoundTripWithDiscoveryInProgress() throws IOException {
        OceanBaseSplit pending =
                new OceanBaseSplit("0", "test_schema", "test_table", "name", null, "M");

        OceanBaseEnumeratorState original =
                new OceanBaseEnumeratorState(new ArrayList<>(), Arrays.asList(pending), false);

        OceanBaseEnumeratorStateSerializer serializer = new OceanBaseEnumeratorStateSerializer();
        byte[] bytes = serializer.serialize(original);
        OceanBaseEnumeratorState restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(0, restored.getInFlightSplits().size());
        assertEquals(1, restored.getPendingSplits().size());
        assertFalse(restored.isSplitDiscoveryFinished());
    }

    @Test
    public void testV3DeserializationDefaultsDiscoveryFinished() throws IOException {
        // Simulate v3 serialized  just two empty split lists, no boolean
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream out = new java.io.DataOutputStream(baos);
        out.writeInt(0); // inFlightSplits size
        out.writeInt(0); // pendingSplits size
        byte[] v3Bytes = baos.toByteArray();

        OceanBaseEnumeratorStateSerializer serializer = new OceanBaseEnumeratorStateSerializer();
        OceanBaseEnumeratorState restored = serializer.deserialize(3, v3Bytes);

        assertEquals(0, restored.getInFlightSplits().size());
        assertEquals(0, restored.getPendingSplits().size());
        assertTrue(restored.isSplitDiscoveryFinished());
    }

    @Test
    public void testFullCheckpointRestoreWithMixedBoundaryTypes() throws IOException {
        OceanBaseSplit longSplit = new OceanBaseSplit("0", "db", "orders", "id", 1L, 1000L);
        OceanBaseSplit stringSplit =
                new OceanBaseSplit("1", "db", "orders", "ROWID", "AAAB", "AAAC");
        OceanBaseSplit decimalSplit =
                new OceanBaseSplit(
                        "2",
                        "db",
                        "orders",
                        "amount",
                        new BigDecimal("100.50"),
                        new BigDecimal("999.99"));
        OceanBaseSplit nullColumnSplit = new OceanBaseSplit("3", "db", "orders", null, null, null);

        OceanBaseEnumeratorState original =
                new OceanBaseEnumeratorState(
                        Arrays.asList(longSplit, stringSplit),
                        Arrays.asList(decimalSplit, nullColumnSplit));

        OceanBaseEnumeratorStateSerializer serializer = new OceanBaseEnumeratorStateSerializer();
        byte[] bytes = serializer.serialize(original);
        OceanBaseEnumeratorState restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(2, restored.getInFlightSplits().size());
        assertEquals(2, restored.getPendingSplits().size());

        assertSplitEquals(longSplit, restored.getInFlightSplits().get(0));
        assertSplitEquals(stringSplit, restored.getInFlightSplits().get(1));
        assertSplitEquals(decimalSplit, restored.getPendingSplits().get(0));
        assertSplitEquals(nullColumnSplit, restored.getPendingSplits().get(1));
    }

    @Test
    public void testStateRoundTripWithLastReadValue() throws IOException {
        OceanBaseSplit inFlight =
                new OceanBaseSplit("1", "test_schema", "test_table", "id", 10L, 20L);
        inFlight.setLastReadValue(15L);

        OceanBaseEnumeratorState original =
                new OceanBaseEnumeratorState(Arrays.asList(inFlight), new ArrayList<>());

        OceanBaseEnumeratorStateSerializer serializer = new OceanBaseEnumeratorStateSerializer();
        byte[] bytes = serializer.serialize(original);
        OceanBaseEnumeratorState restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(1, restored.getInFlightSplits().size());
        assertSplitEquals(inFlight, restored.getInFlightSplits().get(0));
        assertEquals(15L, restored.getInFlightSplits().get(0).getLastReadValue());
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
        assertEquals(expected.getLastReadValue(), actual.getLastReadValue());
    }
}
