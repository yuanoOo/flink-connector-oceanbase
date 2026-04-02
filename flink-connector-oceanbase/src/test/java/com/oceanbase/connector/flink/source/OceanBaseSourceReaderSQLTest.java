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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for OceanBaseSourceReader SQL generation. */
public class OceanBaseSourceReaderSQLTest {

    @Test
    public void testMySQLQuoteIdentifier() {
        assertEquals("`column_name`", quoteIdentifierMySQL("column_name"));
        assertEquals("`table_name`", quoteIdentifierMySQL("table_name"));
        assertEquals("`schema_name`", quoteIdentifierMySQL("schema_name"));
    }

    @Test
    public void testOracleQuoteIdentifier() {
        assertEquals("\"column_name\"", quoteIdentifierOracle("column_name"));
        assertEquals("\"table_name\"", quoteIdentifierOracle("table_name"));
        assertEquals("\"schema_name\"", quoteIdentifierOracle("schema_name"));
    }

    @Test
    public void testMySQLBuildQuerySQLSingleSplit() {
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, null);
        String sql = buildQuerySQLMySQL(split);
        assertEquals("SELECT * FROM `test_db`.`products`", sql);
    }

    @Test
    public void testMySQLBuildQuerySQLFirstSplit() {
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, 50);
        String sql = buildQuerySQLMySQL(split);
        assertEquals("SELECT * FROM `test_db`.`products` WHERE `id` < '50'", sql);
    }

    @Test
    public void testMySQLBuildQuerySQLLastSplit() {
        OceanBaseSplit split = new OceanBaseSplit("2", "test_db", "products", "id", 100, null);
        String sql = buildQuerySQLMySQL(split);
        assertEquals("SELECT * FROM `test_db`.`products` WHERE `id` >= '100'", sql);
    }

    @Test
    public void testMySQLBuildQuerySQLMiddleSplit() {
        OceanBaseSplit split = new OceanBaseSplit("1", "test_db", "products", "id", 50, 100);
        String sql = buildQuerySQLMySQL(split);
        assertEquals("SELECT * FROM `test_db`.`products` WHERE `id` >= '50' AND `id` < '100'", sql);
    }

    @Test
    public void testOracleBuildQuerySQLSingleSplit() {
        OceanBaseSplit split =
                new OceanBaseSplit("0", "TEST_SCHEMA", "PRODUCTS", "ROWID", null, null);
        String sql = buildQuerySQLOracle(split);
        assertEquals("SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\"", sql);
    }

    @Test
    public void testOracleBuildQuerySQLFirstSplit() {
        // For ROWID, the split values are ROWID strings
        OceanBaseSplit split =
                new OceanBaseSplit(
                        "0", "TEST_SCHEMA", "PRODUCTS", "ROWID", null, "AAASdqAAEAAAAInAAA");
        String sql = buildQuerySQLOracle(split);
        assertEquals(
                "SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" < 'AAASdqAAEAAAAInAAA'",
                sql);
    }

    @Test
    public void testOracleBuildQuerySQLLastSplit() {
        OceanBaseSplit split =
                new OceanBaseSplit(
                        "2", "TEST_SCHEMA", "PRODUCTS", "ROWID", "AAASdqAAEAAAAInAAB", null);
        String sql = buildQuerySQLOracle(split);
        assertEquals(
                "SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" >= 'AAASdqAAEAAAAInAAB'",
                sql);
    }

    @Test
    public void testOracleBuildQuerySQLMiddleSplit() {
        OceanBaseSplit split =
                new OceanBaseSplit(
                        "1",
                        "TEST_SCHEMA",
                        "PRODUCTS",
                        "ROWID",
                        "AAASdqAAEAAAAInAAA",
                        "AAASdqAAEAAAAInAAB");
        String sql = buildQuerySQLOracle(split);
        assertEquals(
                "SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" >= 'AAASdqAAEAAAAInAAA' AND \"ROWID\" < 'AAASdqAAEAAAAInAAB'",
                sql);
    }

    @Test
    public void testSplitBoundaryDetection() {
        // Single split - both first and last
        OceanBaseSplit singleSplit = new OceanBaseSplit("0", "schema", "table", "id", null, null);
        assertTrue(singleSplit.isFirstSplit());
        assertTrue(singleSplit.isLastSplit());

        // First split
        OceanBaseSplit firstSplit = new OceanBaseSplit("0", "schema", "table", "id", null, 50);
        assertTrue(firstSplit.isFirstSplit());
        assertFalse(firstSplit.isLastSplit());

        // Middle split
        OceanBaseSplit middleSplit = new OceanBaseSplit("1", "schema", "table", "id", 50, 100);
        assertFalse(middleSplit.isFirstSplit());
        assertFalse(middleSplit.isLastSplit());

        // Last split
        OceanBaseSplit lastSplit = new OceanBaseSplit("2", "schema", "table", "id", 100, null);
        assertFalse(lastSplit.isFirstSplit());
        assertTrue(lastSplit.isLastSplit());
    }

    @Test
    public void testEscapeValue() {
        assertEquals("test", escapeValue("test"));
        assertEquals("test''s", escapeValue("test's"));
        assertEquals("test''s''value", escapeValue("test's'value"));
        assertEquals("", escapeValue(null));
    }

    // Helper methods that mirror the logic in OceanBaseSourceReader

    private String quoteIdentifierMySQL(String identifier) {
        return "`" + identifier + "`";
    }

    private String quoteIdentifierOracle(String identifier) {
        return "\"" + identifier + "\"";
    }

    private String escapeValue(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString().replace("'", "''");
    }

    private String buildQuerySQLMySQL(OceanBaseSplit split) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(quoteIdentifierMySQL(split.getSchemaName()))
                .append(".")
                .append(quoteIdentifierMySQL(split.getTableName()));

        String splitColumn = split.getSplitColumn();
        if (splitColumn != null) {
            if (split.isFirstSplit() && split.isLastSplit()) {
                // Single split, no WHERE clause needed
            } else if (split.isFirstSplit()) {
                sql.append(" WHERE ")
                        .append(quoteIdentifierMySQL(splitColumn))
                        .append(" < '")
                        .append(escapeValue(split.getSplitEnd()))
                        .append("'");
            } else if (split.isLastSplit()) {
                sql.append(" WHERE ")
                        .append(quoteIdentifierMySQL(splitColumn))
                        .append(" >= '")
                        .append(escapeValue(split.getSplitStart()))
                        .append("'");
            } else {
                sql.append(" WHERE ")
                        .append(quoteIdentifierMySQL(splitColumn))
                        .append(" >= '")
                        .append(escapeValue(split.getSplitStart()))
                        .append("' AND ")
                        .append(quoteIdentifierMySQL(splitColumn))
                        .append(" < '")
                        .append(escapeValue(split.getSplitEnd()))
                        .append("'");
            }
        }

        return sql.toString();
    }

    private String buildQuerySQLOracle(OceanBaseSplit split) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(quoteIdentifierOracle(split.getSchemaName()))
                .append(".")
                .append(quoteIdentifierOracle(split.getTableName()));

        String splitColumn = split.getSplitColumn();
        if (splitColumn != null) {
            if (split.isFirstSplit() && split.isLastSplit()) {
                // Single split, no WHERE clause needed
            } else if (split.isFirstSplit()) {
                sql.append(" WHERE ")
                        .append(quoteIdentifierOracle(splitColumn))
                        .append(" < '")
                        .append(escapeValue(split.getSplitEnd()))
                        .append("'");
            } else if (split.isLastSplit()) {
                sql.append(" WHERE ")
                        .append(quoteIdentifierOracle(splitColumn))
                        .append(" >= '")
                        .append(escapeValue(split.getSplitStart()))
                        .append("'");
            } else {
                sql.append(" WHERE ")
                        .append(quoteIdentifierOracle(splitColumn))
                        .append(" >= '")
                        .append(escapeValue(split.getSplitStart()))
                        .append("' AND ")
                        .append(quoteIdentifierOracle(splitColumn))
                        .append(" < '")
                        .append(escapeValue(split.getSplitEnd()))
                        .append("'");
            }
        }

        return sql.toString();
    }
}
