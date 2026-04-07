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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for OceanBaseSourceReader SQL generation strategy. */
public class OceanBaseSourceReaderSQLTest {

    private static final DataType PRODUCED_TYPE =
            DataTypes.ROW(DataTypes.FIELD("f", DataTypes.STRING()));

    private static OceanBaseSourceReader createMySQLReader() {
        OceanBaseSourceConfig config =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "test_db",
                        "products",
                        "MySQL",
                        8192,
                        "id",
                        1024);
        return new OceanBaseSourceReader(null, config, PRODUCED_TYPE);
    }

    private static OceanBaseSourceReader createOracleReader() {
        // Default: oracleTenantCaseInsensitive = true (no quoting)
        OceanBaseSourceConfig config =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "TEST_SCHEMA",
                        "PRODUCTS",
                        "Oracle",
                        8192,
                        "ROWID",
                        1024);
        return new OceanBaseSourceReader(null, config, PRODUCED_TYPE);
    }

    private static OceanBaseSourceReader createOracleCaseSensitiveReader() {
        OceanBaseSourceConfig config =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "TEST_SCHEMA",
                        "PRODUCTS",
                        "Oracle",
                        8192,
                        "ROWID",
                        1024,
                        false,
                        null,
                        null);
        return new OceanBaseSourceReader(null, config, PRODUCED_TYPE);
    }

    @Test
    public void testMySQLQueryNoBoundary() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, null);
        assertEquals(
                "SELECT * FROM `test_db`.`products` ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertEquals(0, reader.buildQueryParamsForTest(split).length);
    }

    @Test
    public void testMySQLQueryFirstSplit() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, 50);
        assertEquals(
                "SELECT * FROM `test_db`.`products` WHERE `id` < ? ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {"50"}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testMySQLQueryLastSplit() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("2", "test_db", "products", "id", 100, null);
        assertEquals(
                "SELECT * FROM `test_db`.`products` WHERE `id` >= ? ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {"100"}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testMySQLQueryMiddleSplit() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("1", "test_db", "products", "id", 50, 100);
        assertEquals(
                "SELECT * FROM `test_db`.`products` WHERE `id` >= ? AND `id` < ? ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {"50", "100"}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testMySQLQueryNullSplitColumn() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", null, null, null);
        assertEquals("SELECT * FROM `test_db`.`products`", reader.buildQueryForTest(split));
        assertEquals(0, reader.buildQueryParamsForTest(split).length);
    }

    @Test
    public void testOracleQueryNoBoundary() {
        OceanBaseSourceReader reader = createOracleReader();
        OceanBaseSplit split =
                new OceanBaseSplit("0", "TEST_SCHEMA", "PRODUCTS", "ROWID", null, null);
        assertEquals(
                "SELECT * FROM TEST_SCHEMA.PRODUCTS ORDER BY ROWID ASC",
                reader.buildQueryForTest(split));
        assertEquals(0, reader.buildQueryParamsForTest(split).length);
    }

    @Test
    public void testOracleQueryFirstSplit() {
        OceanBaseSourceReader reader = createOracleReader();
        String end = "AAASdqAAEAAAAInAAA";
        OceanBaseSplit split =
                new OceanBaseSplit("0", "TEST_SCHEMA", "PRODUCTS", "ROWID", null, end);
        assertEquals(
                "SELECT * FROM TEST_SCHEMA.PRODUCTS WHERE ROWID < ? ORDER BY ROWID ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {end}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testOracleQueryLastSplit() {
        OceanBaseSourceReader reader = createOracleReader();
        String start = "AAASdqAAEAAAAInAAB";
        OceanBaseSplit split =
                new OceanBaseSplit("2", "TEST_SCHEMA", "PRODUCTS", "ROWID", start, null);
        assertEquals(
                "SELECT * FROM TEST_SCHEMA.PRODUCTS WHERE ROWID >= ? ORDER BY ROWID ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {start}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testOracleQueryMiddleSplit() {
        OceanBaseSourceReader reader = createOracleReader();
        String start = "AAASdqAAEAAAAInAAA";
        String end = "AAASdqAAEAAAAInAAB";
        OceanBaseSplit split =
                new OceanBaseSplit("1", "TEST_SCHEMA", "PRODUCTS", "ROWID", start, end);
        assertEquals(
                "SELECT * FROM TEST_SCHEMA.PRODUCTS WHERE ROWID >= ? AND ROWID < ? ORDER BY ROWID ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {start, end}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testOracleCaseSensitiveQueryMiddleSplit() {
        OceanBaseSourceReader reader = createOracleCaseSensitiveReader();
        String start = "AAASdqAAEAAAAInAAA";
        String end = "AAASdqAAEAAAAInAAB";
        OceanBaseSplit split =
                new OceanBaseSplit("1", "TEST_SCHEMA", "PRODUCTS", "ROWID", start, end);
        assertEquals(
                "SELECT * FROM \"TEST_SCHEMA\".\"PRODUCTS\" WHERE \"ROWID\" >= ? AND \"ROWID\" < ? ORDER BY \"ROWID\" ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {start, end}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testQuoteIdentifierEscapesSpecialCharacters() {
        OceanBaseSourceConfig mysqlConfig =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "db",
                        "t",
                        "MySQL",
                        8192,
                        null,
                        1024);
        assertEquals("`col``name`", mysqlConfig.quoteIdentifier("col`name"));
        assertEquals("`normal`", mysqlConfig.quoteIdentifier("normal"));

        // Oracle with case-insensitive (default) - no quoting
        OceanBaseSourceConfig oracleConfig =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "db",
                        "t",
                        "Oracle",
                        8192,
                        null,
                        1024);
        assertEquals("normal", oracleConfig.quoteIdentifier("normal"));

        // Oracle with case-sensitive - quotes identifiers
        OceanBaseSourceConfig oracleCaseSensitiveConfig =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "db",
                        "t",
                        "Oracle",
                        8192,
                        null,
                        1024,
                        false,
                        null,
                        null);
        assertEquals("\"col\"\"name\"", oracleCaseSensitiveConfig.quoteIdentifier("col\"name"));
        assertEquals("\"normal\"", oracleCaseSensitiveConfig.quoteIdentifier("normal"));
    }

    @Test
    public void testResumeFromLastReadValue() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("1", "test_db", "products", "id", 50, 100);
        split.setLastReadValue(75);
        // Should use '>' (strict) and lastReadValue instead of splitStart
        assertEquals(
                "SELECT * FROM `test_db`.`products` WHERE `id` > ? AND `id` < ? ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {"75", "100"}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testResumeFromLastReadValueLastSplit() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("2", "test_db", "products", "id", 100, null);
        split.setLastReadValue(150);
        assertEquals(
                "SELECT * FROM `test_db`.`products` WHERE `id` > ? ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {"150"}, reader.buildQueryParamsForTest(split));
    }

    @Test
    public void testResumeFromLastReadValueFirstSplit() {
        OceanBaseSourceReader reader = createMySQLReader();
        OceanBaseSplit split = new OceanBaseSplit("0", "test_db", "products", "id", null, 50);
        split.setLastReadValue(25);
        assertEquals(
                "SELECT * FROM `test_db`.`products` WHERE `id` > ? AND `id` < ? ORDER BY `id` ASC",
                reader.buildQueryForTest(split));
        assertArrayEquals(new String[] {"25", "50"}, reader.buildQueryParamsForTest(split));
    }
}
