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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link OceanBaseSplitSerializer}. */
public class OceanBaseSplitSerializerTest {

    @Test
    public void testSerializeDeserializeNumericBoundaries() throws IOException {
        OceanBaseSplit split = new OceanBaseSplit("0", "schema", "table", "id", 10L, 20L);
        OceanBaseSplitSerializer serializer = new OceanBaseSplitSerializer();
        OceanBaseSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertEquals(split.splitId(), restored.splitId());
        assertEquals(split.getSchemaName(), restored.getSchemaName());
        assertEquals(split.getTableName(), restored.getTableName());
        assertEquals(split.getSplitColumn(), restored.getSplitColumn());
        assertEquals(split.getSplitStart(), restored.getSplitStart());
        assertEquals(split.getSplitEnd(), restored.getSplitEnd());
    }

    @Test
    public void testSerializeDeserializeStringBoundary() throws IOException {
        OceanBaseSplit split =
                new OceanBaseSplit("rowid", "schema", "table", "ROWID", "AAAB", "AAAC");
        OceanBaseSplitSerializer serializer = new OceanBaseSplitSerializer();
        OceanBaseSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertEquals(split.getSplitStart(), restored.getSplitStart());
        assertEquals(split.getSplitEnd(), restored.getSplitEnd());
    }

    @Test
    public void testSerializeDeserializeDecimalBoundary() throws IOException {
        OceanBaseSplit split =
                new OceanBaseSplit(
                        "decimal",
                        "schema",
                        "table",
                        "amount",
                        new BigDecimal("10.5"),
                        new BigDecimal("99.5"));
        OceanBaseSplitSerializer serializer = new OceanBaseSplitSerializer();
        OceanBaseSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertEquals(split.getSplitStart(), restored.getSplitStart());
        assertEquals(split.getSplitEnd(), restored.getSplitEnd());
    }

    @Test
    public void testSerializeDeserializeNullSplitColumn() throws IOException {
        OceanBaseSplit split = new OceanBaseSplit("0", "schema", "table", null, null, null);
        OceanBaseSplitSerializer serializer = new OceanBaseSplitSerializer();
        OceanBaseSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertEquals("0", restored.splitId());
        assertEquals("schema", restored.getSchemaName());
        assertEquals("table", restored.getTableName());
        assertNull(restored.getSplitColumn());
        assertNull(restored.getSplitStart());
        assertNull(restored.getSplitEnd());
    }

    @Test
    public void testSerializeDeserializeDoubleBoundary() throws IOException {
        OceanBaseSplit split = new OceanBaseSplit("1", "schema", "table", "score", 25.0, 75.5);
        OceanBaseSplitSerializer serializer = new OceanBaseSplitSerializer();
        OceanBaseSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertEquals(split.splitId(), restored.splitId());
        assertEquals(split.getSplitColumn(), restored.getSplitColumn());
        assertEquals(25.0, ((Double) restored.getSplitStart()).doubleValue(), 0.001);
        assertEquals(75.5, ((Double) restored.getSplitEnd()).doubleValue(), 0.001);
    }

    @Test
    public void testSerializeDeserializeNullBoundaries() throws IOException {
        OceanBaseSplit split = new OceanBaseSplit("0", "schema", "table", "id", null, null);
        OceanBaseSplitSerializer serializer = new OceanBaseSplitSerializer();
        OceanBaseSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertEquals("id", restored.getSplitColumn());
        assertNull(restored.getSplitStart());
        assertNull(restored.getSplitEnd());
        assertTrue(restored.isFirstSplit());
        assertTrue(restored.isLastSplit());
    }
}
