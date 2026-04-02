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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * E2E tests for OceanBase parallel snapshot read in Oracle compatible mode.
 *
 * <p>Note: This test requires an OceanBase instance running in Oracle compatible mode. OceanBase
 * Community Edition only supports MySQL mode, so this test is disabled by default. To run this
 * test, set up an Oracle-mode OceanBase instance and configure the following environment variables:
 *
 * <ul>
 *   <li>HOST - OceanBase server host
 *   <li>PORT - OceanBase server port (default: 2881)
 *   <li>RPC_PORT - OceanBase RPC port (default: 2882)
 *   <li>CLUSTER_NAME - OceanBase cluster name
 *   <li>SCHEMA_NAME - Schema name (Oracle user)
 *   <li>SYS_USERNAME - System username
 *   <li>SYS_PASSWORD - System password
 *   <li>USER_NAME - Test username (format: user@tenant)
 *   <li>PASSWORD - Test password
 * </ul>
 */
@Disabled("Requires OceanBase Enterprise Edition with Oracle compatible mode")
public class OceanBaseParallelSourceOracleE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseParallelSourceOracleE2eITCase.class);

    private static final String SOURCE_CONNECTOR_NAME = "flink-sql-connector-oceanbase.jar";
    private static final String SINK_CONNECTOR_NAME = "flink-sql-connector-oceanbase.jar";

    // Environment variable names for Oracle mode
    private static final String ENV_HOST = "HOST";
    private static final String ENV_PORT = "PORT";
    private static final String ENV_RPC_PORT = "RPC_PORT";
    private static final String ENV_CLUSTER_NAME = "CLUSTER_NAME";
    private static final String ENV_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String ENV_SYS_USERNAME = "SYS_USERNAME";
    private static final String ENV_SYS_PASSWORD = "SYS_PASSWORD";
    private static final String ENV_USER_NAME = "USER_NAME";
    private static final String ENV_PASSWORD = "PASSWORD";

    // Cached values from environment variables
    private static String host;
    private static int port;
    private static int rpcPort;
    private static String clusterName;
    private static String schemaName;
    private static String sysUsername;
    private static String sysPassword;
    private static String username;
    private static String password;

    static {
        // Initialize from environment variables
        host = System.getenv(ENV_HOST);
        port = Integer.parseInt(System.getenv().getOrDefault(ENV_PORT, "2881"));
        rpcPort = Integer.parseInt(System.getenv().getOrDefault(ENV_RPC_PORT, "2882"));
        clusterName = System.getenv(ENV_CLUSTER_NAME);
        schemaName = System.getenv(ENV_SCHEMA_NAME);
        sysUsername = System.getenv(ENV_SYS_USERNAME);
        sysPassword = System.getenv(ENV_SYS_PASSWORD);
        username = System.getenv(ENV_USER_NAME);
        password = System.getenv(ENV_PASSWORD);
    }

    @BeforeAll
    public static void setup() {
        // Do not start OceanBase CE container - using external Oracle mode instance
        // Flink containers are started by FlinkContainerTestEnvironment.before()
        LOG.info("Using external OceanBase Oracle mode instance at {}:{}", host, port);
        LOG.info("Schema: {}, User: {}", schemaName, username);
    }

    @AfterAll
    public static void tearDown() {
        // Do not stop OceanBase CE container - using external Oracle mode instance
        // Flink containers are stopped by FlinkContainerTestEnvironment.after()
    }

    // Override methods to use environment variables for Oracle mode
    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getRpcPort() {
        return rpcPort;
    }

    @Override
    public String getJdbcUrl() {
        // Oracle mode uses Oracle JDBC URL format
        return "jdbc:oceanbase://"
                + getHost()
                + ":"
                + getPort()
                + "/"
                + getSchemaName()
                + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getSysUsername() {
        return sysUsername;
    }

    @Override
    public String getSysPassword() {
        return sysPassword;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();

        // Create source table with test data (Oracle syntax)
        try (Connection conn =
                        DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            // Oracle uses NUMBER for numeric types
            stmt.execute(
                    "CREATE TABLE source_products ("
                            + "id NUMBER PRIMARY KEY, "
                            + "name VARCHAR2(255), "
                            + "description VARCHAR2(512), "
                            + "weight NUMBER(20, 10))");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(1, 'scooter', 'Small 2-wheel scooter', 3.14)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(2, 'car battery', '12V car battery', 8.1)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(3, '12-pack drill bits', '12-pack of drill bits', 0.8)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(4, 'hammer', '12oz carpenter hammer', 0.75)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(5, 'hammer', '14oz carpenter hammer', 0.875)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(6, 'hammer', '16oz carpenter hammer', 1.0)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(7, 'rocks', 'box of assorted rocks', 5.3)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(8, 'jacket', 'water resistent wind breaker', 0.1)");
            stmt.execute(
                    "INSERT INTO source_products VALUES "
                            + "(9, 'spare tire', '24 inch spare tire', 22.2)");
            stmt.execute(
                    "INSERT INTO source_products VALUES " + "(10, 'bike', 'mountain bike', 15.5)");
            stmt.execute("COMMIT");
        }

        // Create sink table (Oracle syntax)
        try (Connection conn =
                        DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE target_products ("
                            + "id NUMBER PRIMARY KEY, "
                            + "name VARCHAR2(255), "
                            + "description VARCHAR2(512), "
                            + "weight NUMBER(20, 10))");
        }
    }

    @AfterEach
    public void after() throws Exception {
        super.after();

        dropTables("source_products", "target_products");
    }

    @Test
    public void testParallelSnapshotReadWithRowid() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");
        sqlLines.add("SET 'parallelism.default' = '4';");

        // Create source table with Oracle mode - ROWID is used by default for splitting
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
                                + "  'compatible-mode'='Oracle',"
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
    public void testParallelSnapshotReadWithChunkKeyColumn() throws Exception {
        List<String> sqlLines = new ArrayList<>();

        sqlLines.add("SET 'execution.checkpointing.interval' = '3s';");
        sqlLines.add("SET 'parallelism.default' = '4';");

        // Create source table with Oracle mode and explicit chunk-key-column
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
                                + "  'compatible-mode'='Oracle',"
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
