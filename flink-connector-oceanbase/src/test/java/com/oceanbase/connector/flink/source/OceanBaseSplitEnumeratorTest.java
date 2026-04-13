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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for OceanBaseSplitEnumerator. */
public class OceanBaseSplitEnumeratorTest {

    private static final DataType PRODUCED_TYPE =
            DataTypes.ROW(DataTypes.FIELD("f", DataTypes.STRING()));

    @Test
    public void testBigDecimalSplitPointsKeepDecimalPrecision() {
        OceanBaseSplitEnumerator enumerator = createEnumerator("MySQL", "id");
        List<Object> points =
                enumerator.generateSplitPointsForTest(
                        "amount", new BigDecimal("0.00"), new BigDecimal("10.00"), 4);

        assertEquals(5, points.size());
        assertNull(points.get(0));
        assertNull(points.get(points.size() - 1));

        assertTrue(points.get(1) instanceof BigDecimal);
        assertTrue(points.get(2) instanceof BigDecimal);
        assertTrue(points.get(3) instanceof BigDecimal);

        assertEquals(0, new BigDecimal("2.5").compareTo((BigDecimal) points.get(1)));
        assertEquals(0, new BigDecimal("5.0").compareTo((BigDecimal) points.get(2)));
        assertEquals(0, new BigDecimal("7.5").compareTo((BigDecimal) points.get(3)));
    }

    @Test
    public void testOracleModeDefaultSplitColumn() {
        OceanBaseSourceConfig config = createConfig("Oracle", null);
        assertTrue(config.isOracleMode());
        assertFalse(config.isMySqlMode());
    }

    @Test
    public void testMySQLModeDefaultSplitColumn() {
        OceanBaseSourceConfig config = createConfig("MySQL", null);
        assertFalse(config.isOracleMode());
        assertTrue(config.isMySqlMode());
    }

    @Test
    public void testChunkKeyColumnOverride() {
        OceanBaseSourceConfig config = createConfig("MySQL", "custom_column");
        assertEquals("custom_column", config.getChunkKeyColumn());
    }

    @Test
    public void testSplitIdentifierQuotingViaConfig() {
        OceanBaseSourceConfig mysqlConfig = createConfig("MySQL", null);
        assertEquals("`column_name`", mysqlConfig.quoteIdentifier("column_name"));

        // Oracle default: case-insensitive, no quoting
        OceanBaseSourceConfig oracleConfig = createConfig("Oracle", null);
        assertEquals("column_name", oracleConfig.quoteIdentifier("column_name"));
    }

    @Test
    public void testSplitBoundaryConditions() {
        OceanBaseSplit firstSplit =
                new OceanBaseSplit("0", "test_schema", "test_table", "id", null, 50);
        assertTrue(firstSplit.isFirstSplit());
        assertFalse(firstSplit.isLastSplit());

        OceanBaseSplit middleSplit =
                new OceanBaseSplit("1", "test_schema", "test_table", "id", 50, 100);
        assertFalse(middleSplit.isFirstSplit());
        assertFalse(middleSplit.isLastSplit());

        OceanBaseSplit lastSplit =
                new OceanBaseSplit("2", "test_schema", "test_table", "id", 100, null);
        assertFalse(lastSplit.isFirstSplit());
        assertTrue(lastSplit.isLastSplit());

        OceanBaseSplit singleSplit =
                new OceanBaseSplit("0", "test_schema", "test_table", "id", null, null);
        assertTrue(singleSplit.isFirstSplit());
        assertTrue(singleSplit.isLastSplit());
    }

    @Test
    public void testSingleSplitWhenNoSplitColumn() {
        OceanBaseSourceReader reader =
                new OceanBaseSourceReader(null, createConfig("MySQL", null), PRODUCED_TYPE);
        OceanBaseSplit split =
                new OceanBaseSplit("0", "test_schema", "test_table", null, null, null);
        String sql = reader.buildQueryForTest(split);
        assertEquals("SELECT * FROM `test_schema`.`test_table`", sql);
        assertEquals(0, reader.buildQueryParamsForTest(split).length);
    }

    @Test
    public void testSingleSplitWithSplitColumn() {
        OceanBaseSourceReader reader =
                new OceanBaseSourceReader(null, createConfig("MySQL", "id"), PRODUCED_TYPE);
        OceanBaseSplit split =
                new OceanBaseSplit("0", "test_schema", "test_table", "id", null, null);
        String sql = reader.buildQueryForTest(split);
        assertEquals("SELECT * FROM `test_schema`.`test_table` ORDER BY `id` ASC", sql);
        assertEquals(0, reader.buildQueryParamsForTest(split).length);
    }

    @Test
    public void testIntegerSplitPoints() {
        OceanBaseSplitEnumerator enumerator = createEnumerator("MySQL", "id");
        List<Object> points = enumerator.generateSplitPointsForTest("id", 0, 100, 4);

        assertEquals(5, points.size());
        assertNull(points.get(0));
        assertNull(points.get(points.size() - 1));

        assertTrue(points.get(1) instanceof Long);
        assertEquals(25L, points.get(1));
        assertEquals(50L, points.get(2));
        assertEquals(75L, points.get(3));
    }

    @Test
    public void testLongSplitPointsNoPrecisionLoss() {
        OceanBaseSplitEnumerator enumerator = createEnumerator("MySQL", "id");
        long min = (1L << 53) + 100;
        long max = (1L << 53) + 500;
        List<Object> points = enumerator.generateSplitPointsForTest("id", min, max, 4);

        assertEquals(5, points.size());
        assertNull(points.get(0));
        assertNull(points.get(points.size() - 1));

        assertTrue(points.get(1) instanceof Long);
        assertEquals(min + 100L, points.get(1));
        assertEquals(min + 200L, points.get(2));
        assertEquals(min + 300L, points.get(3));
    }

    @Test
    public void testDoubleSplitPoints() {
        OceanBaseSplitEnumerator enumerator = createEnumerator("MySQL", "id");
        List<Object> points = enumerator.generateSplitPointsForTest("id", 0.0, 100.0, 4);

        assertEquals(5, points.size());
        assertNull(points.get(0));
        assertNull(points.get(points.size() - 1));

        assertTrue(points.get(1) instanceof Double);
        assertEquals(25.0, ((Double) points.get(1)).doubleValue(), 0.001);
        assertEquals(50.0, ((Double) points.get(2)).doubleValue(), 0.001);
        assertEquals(75.0, ((Double) points.get(3)).doubleValue(), 0.001);
    }

    @Test
    public void testLongSplitPointsFullRangeNoOverflow() {
        OceanBaseSplitEnumerator enumerator = createEnumerator("MySQL", "id");
        // Full BIGINT range: Long.MIN_VALUE to Long.MAX_VALUE
        List<Object> points =
                enumerator.generateSplitPointsForTest("id", Long.MIN_VALUE, Long.MAX_VALUE, 4);

        assertEquals(5, points.size());
        assertNull(points.get(0));
        assertNull(points.get(points.size() - 1));

        // Verify split points are monotonically increasing and within range
        for (int i = 1; i < points.size() - 1; i++) {
            assertTrue(points.get(i) instanceof Long);
        }
        long p1 = (Long) points.get(1);
        long p2 = (Long) points.get(2);
        long p3 = (Long) points.get(3);

        assertTrue(Long.MIN_VALUE < p1, "First split point should be > Long.MIN_VALUE");
        assertTrue(p1 < p2, "Split points should be monotonically increasing");
        assertTrue(p2 < p3, "Split points should be monotonically increasing");
        assertTrue(p3 < Long.MAX_VALUE, "Last split point should be < Long.MAX_VALUE");
    }

    @Test
    public void testLongSplitPointsLargeRangeNoOverflow() {
        OceanBaseSplitEnumerator enumerator = createEnumerator("MySQL", "id");
        // Large range that would overflow with naive long multiplication
        long min = 0L;
        long max = Long.MAX_VALUE;
        List<Object> points = enumerator.generateSplitPointsForTest("id", min, max, 4);

        assertEquals(5, points.size());
        assertNull(points.get(0));
        assertNull(points.get(points.size() - 1));

        long p1 = (Long) points.get(1);
        long p2 = (Long) points.get(2);
        long p3 = (Long) points.get(3);

        assertTrue(p1 > 0, "Split points should be positive");
        assertTrue(p1 < p2);
        assertTrue(p2 < p3);
        assertTrue(p3 < Long.MAX_VALUE);
    }

    // Helper methods

    private static OceanBaseSourceConfig createConfig(
            String compatibleMode, String chunkKeyColumn) {
        return new OceanBaseSourceConfig(
                "jdbc:oceanbase://127.0.0.1:2881/test",
                "user",
                "pwd",
                "test_schema",
                "test_table",
                compatibleMode,
                8192,
                chunkKeyColumn,
                1024);
    }

    private static OceanBaseSplitEnumerator createEnumerator(
            String compatibleMode, String chunkKeyColumn) {
        OceanBaseSourceConfig config = createConfig(compatibleMode, chunkKeyColumn);
        return new OceanBaseSplitEnumerator(null, config, null);
    }
}
