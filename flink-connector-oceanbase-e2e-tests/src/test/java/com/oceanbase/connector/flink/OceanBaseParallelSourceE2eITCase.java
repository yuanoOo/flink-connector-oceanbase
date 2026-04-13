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

package com.oceanbase.connector.flink;

import com.oceanbase.connector.flink.utils.FlinkContainerTestEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OceanBaseParallelSourceE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseParallelSourceE2eITCase.class);

    private static final String SOURCE_CONNECTOR_NAME = "flink-sql-connector-oceanbase.jar";
    private static final String SINK_CONNECTOR_NAME = "flink-sql-connector-oceanbase.jar";

    @BeforeAll
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    @AfterAll
    public static void tearDown() {
        CONTAINER.stop();
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();

        // Create source table with test data
        try (Connection conn =
                        DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS source_products ("
                            + "id INT PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "description VARCHAR(512), "
                            + "weight DECIMAL(20, 10))");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(1, 'scooter', 'Small 2-wheel scooter', 3.14),"
                            + "(2, 'car battery', '12V car battery', 8.1),"
                            + "(3, '12-pack drill bits', '12-pack of drill bits', 0.8),"
                            + "(4, 'hammer', '12oz carpenter hammer', 0.75),"
                            + "(5, 'hammer', '14oz carpenter hammer', 0.875),"
                            + "(6, 'hammer', '16oz carpenter hammer', 1.0),"
                            + "(7, 'rocks', 'box of assorted rocks', 5.3),"
                            + "(8, 'jacket', 'water resistent wind breaker', 0.1),"
                            + "(9, 'spare tire', '24 inch spare tire', 22.2),"
                            + "(10, 'bike', 'mountain bike', 15.5)");
        }

        // Create sink table
        try (Connection conn =
                        DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS target_products ("
                            + "id INT PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "description VARCHAR(512), "
                            + "weight DECIMAL(20, 10))");
        }
    }

    @AfterEach
    public void after() throws Exception {
        super.after();

        dropTables("source_products", "target_products");

        // Clean up no-PK tables if they exist
        try (Connection conn =
                        DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS source_no_pk");
            stmt.execute("DROP TABLE IF EXISTS target_no_pk");
        }
    }

    @Test
    public void testParallelSnapshotRead() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");
        sqlLines.add("SET 'parallelism.default' = '4';");

        // Create source table with parallel read
        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE source_table ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='source_products',"
                                + "  'compatible-mode'='MySQL',"
                                + "  'split-size'='2'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        // Create sink table
        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE target_table ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='target_products'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        sqlLines.add("INSERT INTO target_table SELECT * FROM source_table;");

        submitSQLJob(
                sqlLines, getResource(SOURCE_CONNECTOR_NAME), getResource(SINK_CONNECTOR_NAME));

        List<String> expected =
                Arrays.asList(
                        "1,scooter,Small 2-wheel scooter,3.1400000000",
                        "2,car battery,12V car battery,8.1000000000",
                        "3,12-pack drill bits,12-pack of drill bits,0.8000000000",
                        "4,hammer,12oz carpenter hammer,0.7500000000",
                        "5,hammer,14oz carpenter hammer,0.8750000000",
                        "6,hammer,16oz carpenter hammer,1.0000000000",
                        "7,rocks,box of assorted rocks,5.3000000000",
                        "8,jacket,water resistent wind breaker,0.1000000000",
                        "9,spare tire,24 inch spare tire,22.2000000000",
                        "10,bike,mountain bike,15.5000000000");

        waitingAndAssertTableCount("target_products", expected.size());

        List<String> actual = queryTable("target_products");
        assertEqualsInAnyOrder(expected, actual);
    }

    @Test
    public void testParallelSourceNoPrimaryKey() throws Exception {
        // Create no-PK source table with test data
        try (Connection conn =
                        DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS source_no_pk ("
                            + "name VARCHAR(255), "
                            + "value INT)");
            stmt.execute(
                    "INSERT INTO source_no_pk VALUES "
                            + "('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5),"
                            + "('f', 6), ('g', 7), ('h', 8), ('i', 9), ('j', 10)");

            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS target_no_pk ("
                            + "name VARCHAR(255), "
                            + "value INT)");
        }

        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");
        sqlLines.add("SET 'parallelism.default' = '4';");

        // Source table without PK — connector should auto-detect __pk_increment
        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE source_table ("
                                + " name STRING,"
                                + " `value` INT"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='source_no_pk',"
                                + "  'compatible-mode'='MySQL',"
                                + "  'split-size'='3'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        // Sink table
        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE target_table ("
                                + " name STRING,"
                                + " `value` INT"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='target_no_pk'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        sqlLines.add("INSERT INTO target_table SELECT * FROM source_table;");

        submitSQLJob(
                sqlLines, getResource(SOURCE_CONNECTOR_NAME), getResource(SINK_CONNECTOR_NAME));

        List<String> expected =
                Arrays.asList(
                        "a,1", "b,2", "c,3", "d,4", "e,5", "f,6", "g,7", "h,8", "i,9", "j,10");

        waitingAndAssertTableCount("target_no_pk", expected.size());

        List<String> actual = queryTable("target_no_pk");
        assertEqualsInAnyOrder(expected, actual);
    }

    @Test
    public void testParallelSnapshotReadWithChunkKeyColumn() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");
        sqlLines.add("SET 'parallelism.default' = '4';");

        // Create source table with chunk-key-column specified
        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE source_table ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='source_products',"
                                + "  'compatible-mode'='MySQL',"
                                + "  'split-size'='3',"
                                + "  'chunk-key-column'='id'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        // Create sink table
        sqlLines.add(
                String.format(
                        "CREATE TEMPORARY TABLE target_table ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") with ("
                                + "  'connector'='oceanbase',"
                                + "  'url'='%s',"
                                + "  'username'='%s',"
                                + "  'password'='%s',"
                                + "  'schema-name'='%s',"
                                + "  'table-name'='target_products'"
                                + ");",
                        getJdbcUrl(), getUsername(), getPassword(), getSchemaName()));

        sqlLines.add("INSERT INTO target_table SELECT * FROM source_table;");

        submitSQLJob(
                sqlLines, getResource(SOURCE_CONNECTOR_NAME), getResource(SINK_CONNECTOR_NAME));

        List<String> expected =
                Arrays.asList(
                        "1,scooter,Small 2-wheel scooter,3.1400000000",
                        "2,car battery,12V car battery,8.1000000000",
                        "3,12-pack drill bits,12-pack of drill bits,0.8000000000",
                        "4,hammer,12oz carpenter hammer,0.7500000000",
                        "5,hammer,14oz carpenter hammer,0.8750000000",
                        "6,hammer,16oz carpenter hammer,1.0000000000",
                        "7,rocks,box of assorted rocks,5.3000000000",
                        "8,jacket,water resistent wind breaker,0.1000000000",
                        "9,spare tire,24 inch spare tire,22.2000000000",
                        "10,bike,mountain bike,15.5000000000");

        waitingAndAssertTableCount("target_products", expected.size());

        List<String> actual = queryTable("target_products");
        assertEqualsInAnyOrder(expected, actual);
    }
}
