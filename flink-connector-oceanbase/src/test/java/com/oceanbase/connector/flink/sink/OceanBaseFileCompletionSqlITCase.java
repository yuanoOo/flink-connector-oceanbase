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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SQL-level integration test for the file-completion Kafka notification feature.
 *
 * <p>Drives the full Flink SQL path: {@code CREATE TABLE ... WITH ('connector'='oceanbase',
 * 'file-completion.*' = ...) ; INSERT INTO ...} so factory option parsing, schema validation, sink
 * runtime wiring and completion-row handling are all exercised end-to-end. OB writes hit a real
 * remote instance; Kafka producer is replaced by an in-memory {@link MockProducer} via {@link
 * KafkaFileCompletionNotifier#producerFactoryForTest}, so no broker is required.
 *
 * <p>Connector option validation and column typing are covered by {@link
 * com.oceanbase.connector.flink.OceanBaseConnectorOptionsFileCompletionTest} and {@link
 * FileCompletionColumnsTest}; this class focuses on the SQL runtime path against a real OB
 * instance.
 *
 * <p>Requires a reachable OceanBase instance. Connection settings must come from environment
 * variables (no credentials in source). If {@value ItEnv#JDBC_URL} or {@value ItEnv#USERNAME} is
 * unset or blank, each test method is skipped via {@link Assumptions} (suitable for public CI
 * without secrets).
 *
 * <ul>
 *   <li>{@value ItEnv#JDBC_URL} — full JDBC URL including database path and query parameters.
 *   <li>{@value ItEnv#USERNAME} — JDBC username.
 *   <li>{@value ItEnv#PASSWORD} — optional JDBC password; if unset, treated as empty.
 *   <li>{@value ItEnv#SCHEMA} — optional; sink {@code schema-name}, default {@code test}.
 *   <li>{@value ItEnv#TABLE} — optional; physical table name for setup and sink, default {@code
 *       t_file_completion_it}.
 * </ul>
 */
class OceanBaseFileCompletionSqlITCase {

    /** Env var names for {@link System#getenv(String)}. */
    static final class ItEnv {
        static final String JDBC_URL = "OCEANBASE_FILE_COMPLETION_IT_JDBC_URL";
        static final String USERNAME = "OCEANBASE_FILE_COMPLETION_IT_USERNAME";
        static final String PASSWORD = "OCEANBASE_FILE_COMPLETION_IT_PASSWORD";
        static final String SCHEMA = "OCEANBASE_FILE_COMPLETION_IT_SCHEMA";
        static final String TABLE = "OCEANBASE_FILE_COMPLETION_IT_TABLE";

        private ItEnv() {}
    }

    /** Filled in {@link #createTable()} when IT env is configured; otherwise left null. */
    private static String jdbcUrl;

    private static String jdbcUser;
    private static String jdbcPassword;
    private static String obSchema;
    private static String obTable;

    /** Captured by the injected MockProducer, shared across the local Flink task threads. */
    private static MockProducer<String, String> mockProducer;

    @BeforeAll
    static void createTable() throws SQLException {
        if (!itEnvPresent()) {
            return;
        }
        jdbcUrl = System.getenv(ItEnv.JDBC_URL);
        jdbcUser = System.getenv(ItEnv.USERNAME);
        jdbcPassword = System.getenv(ItEnv.PASSWORD) != null ? System.getenv(ItEnv.PASSWORD) : "";
        obSchema = firstNonBlank(System.getenv(ItEnv.SCHEMA), "test");
        obTable = firstNonBlank(System.getenv(ItEnv.TABLE), "t_file_completion_it");
        try (Connection conn = jdbc();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + obTable);
            st.execute(
                    "CREATE TABLE "
                            + obTable
                            + " (id INT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(10,2))");
        }
    }

    @AfterAll
    static void dropTable() throws SQLException {
        if (jdbcUrl == null || obTable == null) {
            return;
        }
        try (Connection conn = jdbc();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + obTable);
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        Assumptions.assumeTrue(
                itEnvPresent(),
                () ->
                        "skip: set non-blank "
                                + ItEnv.JDBC_URL
                                + " and "
                                + ItEnv.USERNAME
                                + " to run this integration test");
        try (Connection conn = jdbc();
                Statement st = conn.createStatement()) {
            st.execute("TRUNCATE TABLE " + obTable);
        }
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaFileCompletionNotifier.producerFactoryForTest = props -> mockProducer;
    }

    @AfterEach
    void tearDown() {
        KafkaFileCompletionNotifier.producerFactoryForTest = null;
        if (mockProducer != null) {
            mockProducer.close();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // happy path: file completion row triggers exactly one Kafka send
    // ---------------------------------------------------------------------------------------------

    @Test
    void completionRowTriggersKafkaSendAndWritesBusinessRowsToOb() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv();

        tEnv.executeSql(buildSinkDdl());
        tEnv.executeSql(
                        "INSERT INTO ob_sink VALUES "
                                + " (1, CAST('scooter' AS STRING), CAST(3.14 AS DECIMAL(10,2)), false, CAST(NULL AS STRING)),"
                                + " (2, CAST('battery' AS STRING), CAST(8.10 AS DECIMAL(10,2)), false, CAST(NULL AS STRING)),"
                                + " (3, CAST('drill'   AS STRING), CAST(0.80 AS DECIMAL(10,2)), false, CAST(NULL AS STRING)),"
                                + " (4, CAST('hammer'  AS STRING), CAST(0.75 AS DECIMAL(10,2)), true,  CAST('oss://b/file_001.parquet#done' AS STRING))")
                .await();

        assertEquals(
                Arrays.asList("1|scooter|3.14", "2|battery|8.10", "3|drill|0.80", "4|hammer|0.75"),
                queryOrderedById());

        List<ProducerRecord<String, String>> sent = mockProducer.history();
        assertEquals(1, sent.size());
        assertEquals("oss-file-events", sent.get(0).topic());
        assertEquals("oss://b/file_001.parquet#done", sent.get(0).value());
    }

    @Test
    void noCompletionRowMeansNoKafkaSend() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv();

        tEnv.executeSql(buildSinkDdl());
        tEnv.executeSql(
                        "INSERT INTO ob_sink VALUES "
                                + " (10, CAST('a' AS STRING), CAST(1.00 AS DECIMAL(10,2)), false, CAST(NULL AS STRING)),"
                                + " (11, CAST('b' AS STRING), CAST(2.00 AS DECIMAL(10,2)), false, CAST(NULL AS STRING))")
                .await();

        assertEquals(2, queryOrderedById().size());
        assertTrue(mockProducer.history().isEmpty());
    }

    @Test
    void notificationDisabledWritesToObWithoutKafkaDespitePartialFileCompletionOptions()
            throws Exception {
        StreamTableEnvironment tEnv = newTableEnv();

        tEnv.executeSql(buildSinkDdlNotificationDisabled());
        tEnv.executeSql(
                        "INSERT INTO ob_sink VALUES "
                                + " (40, CAST('plain' AS STRING), CAST(4.00 AS DECIMAL(10,2))),"
                                + " (41, CAST('sink'  AS STRING), CAST(5.00 AS DECIMAL(10,2)))")
                .await();

        assertEquals(Arrays.asList("40|plain|4.00", "41|sink|5.00"), queryOrderedById());
        assertTrue(mockProducer.history().isEmpty());
    }

    @Test
    void kafkaSendFailureBubblesUpAsJobFailure() throws Exception {
        MockProducer<String, String> failing =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        failing.sendException = new RuntimeException("kafka boom");
        mockProducer = failing;
        KafkaFileCompletionNotifier.producerFactoryForTest = props -> failing;

        StreamTableEnvironment tEnv = newTableEnv();
        tEnv.executeSql(buildSinkDdl());

        Exception ex =
                assertThrows(
                        Exception.class,
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO ob_sink VALUES "
                                                        + " (30, CAST('x' AS STRING), CAST(1.00 AS DECIMAL(10,2)), true, CAST('msg' AS STRING))")
                                        .await());
        assertTrue(
                throwableChainContains(ex, "kafka boom"),
                "expected kafka send failure in cause chain, got: " + ex);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static StreamTableEnvironment newTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static String buildSinkDdl() {
        return buildSinkDdlWithFileCompletionKafka(true);
    }

    /** OB-matching schema; notification off with deliberately incomplete file-completion keys. */
    private static String buildSinkDdlNotificationDisabled() {
        return "CREATE TEMPORARY TABLE ob_sink ("
                + " id INT,"
                + " name STRING,"
                + " amount DECIMAL(10,2),"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + commonSinkOptions()
                + " 'file-completion.kafka.notification-enabled'='false',"
                + " 'file-completion.flag-column'='is_eof',"
                + " 'file-completion.kafka.topic'='oss-file-events'"
                + ")";
    }

    private static String buildSinkDdlWithFileCompletionKafka(boolean notificationEnabled) {
        return "CREATE TEMPORARY TABLE ob_sink ("
                + " id INT,"
                + " name STRING,"
                + " amount DECIMAL(10,2),"
                + " is_eof BOOLEAN,"
                + " kafka_msg STRING,"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + commonSinkOptions()
                + " 'file-completion.flag-column'='is_eof',"
                + " 'file-completion.message-column'='kafka_msg',"
                + " 'file-completion.kafka.notification-enabled'='"
                + notificationEnabled
                + "',"
                + " 'file-completion.kafka.topic'='oss-file-events',"
                + " 'file-completion.kafka.properties.bootstrap.servers'='dummy:9092'"
                + ")";
    }

    private static String commonSinkOptions() {
        return " 'connector'='oceanbase',"
                + " 'url'='"
                + jdbcUrl
                + "',"
                + " 'username'='"
                + jdbcUser
                + "',"
                + " 'password'='"
                + jdbcPassword
                + "',"
                + " 'schema-name'='"
                + obSchema
                + "',"
                + " 'table-name'='"
                + obTable
                + "',"
                + " 'sync-write'='true',";
    }

    private static Connection jdbc() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
    }

    private static List<String> queryOrderedById() throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection conn = jdbc();
                Statement st = conn.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "SELECT id, name, amount FROM " + obTable + " ORDER BY id")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + rs.getBigDecimal(3));
            }
        }
        return out;
    }

    private static boolean throwableChainContains(Throwable t, String needle) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            String m = cur.getMessage();
            if (m != null && m.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private static boolean itEnvPresent() {
        String url = System.getenv(ItEnv.JDBC_URL);
        String user = System.getenv(ItEnv.USERNAME);
        return url != null && !url.trim().isEmpty() && user != null && !user.trim().isEmpty();
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.trim().isEmpty()) {
            return preferred;
        }
        return fallback;
    }
}
