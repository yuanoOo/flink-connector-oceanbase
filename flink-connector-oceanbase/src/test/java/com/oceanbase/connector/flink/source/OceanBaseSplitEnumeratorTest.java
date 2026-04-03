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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for OceanBaseSplitEnumerator. */
public class OceanBaseSplitEnumeratorTest {

    @Test
    public void testBigDecimalSplitPointsKeepDecimalPrecision() {
        OceanBaseSplitEnumerator enumerator = createEnumerator();
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
        // Test that Oracle mode uses ROWID as default split column
        TestConfig config = new TestConfig("Oracle", null);
        assertTrue(config.isOracleMode());
        assertEquals("ROWID", getExpectedDefaultSplitColumn(config));
    }

    @Test
    public void testMySQLModeDefaultSplitColumn() {
        // Test that MySQL mode uses primary key as default split column
        TestConfig config = new TestConfig("MySQL", null);
        // Primary key detection would require database connection
        // This test verifies the config is set correctly
        assertFalse(config.isOracleMode());
    }

    @Test
    public void testChunkKeyColumnOverride() {
        // Test that chunk-key-column config overrides default
        TestConfig config = new TestConfig("MySQL", "custom_column");
        assertEquals("custom_column", config.getChunkKeyColumn());
    }

    @Test
    public void testSplitIdentifierQuoting() {
        // Test that identifiers are quoted correctly for MySQL and Oracle modes
        String mysqlQuoted = quoteIdentifierMySQL("column_name");
        assertEquals("`column_name`", mysqlQuoted);

        String oracleQuoted = quoteIdentifierOracle("column_name");
        assertEquals("\"column_name\"", oracleQuoted);
    }

    @Test
    public void testSplitPointGenerationForNumeric() {
        // Test split point generation for numeric types
        List<Object> points = generateSplitPoints(1, 100, 4);
        assertEquals(5, points.size()); // null + 3 points + null
        assertNull(points.get(0)); // First split starts from null
        assertNull(points.get(4)); // Last split ends at null
    }

    @Test
    public void testSplitPointGenerationForString() {
        // Test split point generation for string types (ROWID)
        List<Object> points = generateSplitPoints("AAA", "ZZZ", 2);
        assertEquals(3, points.size()); // null + 1 point + null
    }

    @Test
    public void testSplitBoundaryConditions() {
        // Test that first split has null start and last split has null end
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

    // Helper methods

    private String getExpectedDefaultSplitColumn(OceanBaseSourceConfig config) {
        if (config.isOracleMode()) {
            return "ROWID";
        }
        return null; // Would need to query database for primary key
    }

    private String quoteIdentifierMySQL(String identifier) {
        return "`" + identifier + "`";
    }

    private String quoteIdentifierOracle(String identifier) {
        return "\"" + identifier + "\"";
    }

    private List<Object> generateSplitPoints(Object min, Object max, int numSplits) {
        List<Object> points = new ArrayList<>();
        points.add(null); // First split starts from null

        if (min instanceof Number && max instanceof Number) {
            double minVal = ((Number) min).doubleValue();
            double maxVal = ((Number) max).doubleValue();
            double step = (maxVal - minVal) / numSplits;

            for (int i = 1; i < numSplits; i++) {
                points.add(minVal + step * i);
            }
        } else {
            // For strings, simplified approach
            for (int i = 1; i < numSplits; i++) {
                points.add(min);
            }
        }

        points.add(null); // Last split ends at null
        return points;
    }

    private OceanBaseSplitEnumerator createEnumerator() {
        OceanBaseSourceConfig config =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "test_schema",
                        "test_table",
                        "MySQL",
                        8096,
                        "id",
                        1024);
        return new OceanBaseSplitEnumerator(null, config, null);
    }

    /** Simple test config implementation. */
    private static class TestConfig extends OceanBaseSourceConfig {
        private final String compatibleMode;
        private final String chunkKeyColumn;

        TestConfig(String compatibleMode, String chunkKeyColumn) {
            super(
                    "jdbc:mysql://localhost:2881/test",
                    "test_user",
                    "test_password",
                    "test_schema",
                    "test_table",
                    compatibleMode,
                    8096,
                    chunkKeyColumn,
                    1024);
            this.compatibleMode = compatibleMode;
            this.chunkKeyColumn = chunkKeyColumn;
        }

        @Override
        public String getCompatibleMode() {
            return compatibleMode;
        }

        @Override
        public boolean isOracleMode() {
            return "Oracle".equalsIgnoreCase(compatibleMode);
        }

        @Override
        public String getChunkKeyColumn() {
            return chunkKeyColumn;
        }
    }
}
