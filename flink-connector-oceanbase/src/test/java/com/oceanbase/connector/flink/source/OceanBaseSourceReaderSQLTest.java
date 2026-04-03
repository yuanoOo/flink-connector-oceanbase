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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for OceanBaseSourceReader SQL generation strategy. */
public class OceanBaseSourceReaderSQLTest {

    @Test
    public void testMySQLQueryNoBoundary() {
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, null);
        QueryPlan plan = buildQuery(split, true);
        assertEquals("SELECT * FROM `test_db`.`products`", plan.sql);
        assertEquals(0, plan.params.size());
    }

    @Test
    public void testMySQLQueryFirstSplit() {
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, 50);
        QueryPlan plan = buildQuery(split, true);
        assertEquals("SELECT * FROM `test_db`.`products` WHERE `id` < ?", plan.sql);
        assertEquals(Arrays.asList("50"), plan.params);
    }

    @Test
    public void testMySQLQueryLastSplit() {
        OceanBaseSplit split = new OceanBaseSplit("2", "test_db", "products", "id", 100, null);
        QueryPlan plan = buildQuery(split, true);
        assertEquals("SELECT * FROM `test_db`.`products` WHERE `id` >= ?", plan.sql);
        assertEquals(Arrays.asList("100"), plan.params);
    }

    @Test
    public void testMySQLQueryMiddleSplit() {
        OceanBaseSplit split = new OceanBaseSplit("1", "test_db", "products", "id", 50, 100);
        QueryPlan plan = buildQuery(split, true);
        assertEquals("SELECT * FROM `test_db`.`products` WHERE `id` >= ? AND `id` < ?", plan.sql);
        assertEquals(Arrays.asList("50", "100"), plan.params);
    }

    @Test
    public void testOracleQueryNoBoundary() {
        OceanBaseSplit split =
                new OceanBaseSplit("0", "TEST_SCHEMA", "PRODUCTS", "ROWID", null, null);
        QueryPlan plan = buildQuery(split, false);
        assertEquals("SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\"", plan.sql);
        assertEquals(0, plan.params.size());
    }

    @Test
    public void testOracleQueryFirstSplit() {
        String end = "AAASdqAAEAAAAInAAA";
        OceanBaseSplit split =
                new OceanBaseSplit("0", "TEST_SCHEMA", "PRODUCTS", "ROWID", null, end);
        QueryPlan plan = buildQuery(split, false);
        assertEquals("SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" < ?", plan.sql);
        assertEquals(Arrays.asList(end), plan.params);
    }

    @Test
    public void testOracleQueryLastSplit() {
        String start = "AAASdqAAEAAAAInAAB";
        OceanBaseSplit split =
                new OceanBaseSplit("2", "TEST_SCHEMA", "PRODUCTS", "ROWID", start, null);
        QueryPlan plan = buildQuery(split, false);
        assertEquals("SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" >= ?", plan.sql);
        assertEquals(Arrays.asList(start), plan.params);
    }

    @Test
    public void testOracleQueryMiddleSplit() {
        String start = "AAASdqAAEAAAAInAAA";
        String end = "AAASdqAAEAAAAInAAB";
        OceanBaseSplit split =
                new OceanBaseSplit("1", "TEST_SCHEMA", "PRODUCTS", "ROWID", start, end);
        QueryPlan plan = buildQuery(split, false);
        assertEquals(
                "SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" >= ? AND \"ROWID\" < ?",
                plan.sql);
        assertEquals(Arrays.asList(start, end), plan.params);
    }

    private QueryPlan buildQuery(OceanBaseSplit split, boolean mysqlMode) {
        List<String> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(
                        mysqlMode
                                ? quoteIdentifierMySQL(split.getSchemaName())
                                : quoteIdentifierOracle(split.getSchemaName()))
                .append(".")
                .append(
                        mysqlMode
                                ? quoteIdentifierMySQL(split.getTableName())
                                : quoteIdentifierOracle(split.getTableName()));

        String splitColumn = split.getSplitColumn();
        if (splitColumn != null) {
            if (!split.isFirstSplit() || !split.isLastSplit()) {
                sql.append(" WHERE ");
                if (split.isFirstSplit()) {
                    sql.append(
                                    mysqlMode
                                            ? quoteIdentifierMySQL(splitColumn)
                                            : quoteIdentifierOracle(splitColumn))
                            .append(" < ?");
                    params.add(String.valueOf(split.getSplitEnd()));
                } else if (split.isLastSplit()) {
                    sql.append(
                                    mysqlMode
                                            ? quoteIdentifierMySQL(splitColumn)
                                            : quoteIdentifierOracle(splitColumn))
                            .append(" >= ?");
                    params.add(String.valueOf(split.getSplitStart()));
                } else {
                    sql.append(
                                    mysqlMode
                                            ? quoteIdentifierMySQL(splitColumn)
                                            : quoteIdentifierOracle(splitColumn))
                            .append(" >= ? AND ")
                            .append(
                                    mysqlMode
                                            ? quoteIdentifierMySQL(splitColumn)
                                            : quoteIdentifierOracle(splitColumn))
                            .append(" < ?");
                    params.add(String.valueOf(split.getSplitStart()));
                    params.add(String.valueOf(split.getSplitEnd()));
                }
            }
        }

        return new QueryPlan(sql.toString(), params);
    }

    private String quoteIdentifierMySQL(String identifier) {
        return "`" + identifier + "`";
    }

    private String quoteIdentifierOracle(String identifier) {
        return "\"" + identifier + "\"";
    }

    private static class QueryPlan {
        private final String sql;
        private final List<String> params;

        private QueryPlan(String sql, List<String> params) {
            this.sql = sql;
            this.params = params;
        }
    }
}
