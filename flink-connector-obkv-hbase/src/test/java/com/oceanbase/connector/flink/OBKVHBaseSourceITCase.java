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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OBKVHBaseSourceITCase extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseSourceITCase.class);

    @BeforeAll
    public static void setup() throws Exception {
        CONFIG_SERVER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();

        CONTAINER
                .withEnv("OB_CONFIGSERVER_ADDRESS", getConfigServerAddress())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();

        String password = "test";
        createSysUser("proxyro", password);

        ODP.withPassword(password)
                .withConfigUrl(getConfigUrlForODP())
                .withLogConsumer(new Slf4jLogConsumer(LOG))
                .start();
    }

    @AfterAll
    public static void tearDown() {
        Stream.of(CONFIG_SERVER, CONTAINER, ODP).forEach(GenericContainer::stop);
    }

    @BeforeEach
    public void before() throws Exception {
        initialize("sql/htable.sql");
    }

    @AfterEach
    public void after() throws Exception {
        dropTables("htable$family1", "htable$family2");
    }

    @Override
    public Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("username", getUsername() + "#" + getClusterName());
        options.put("password", getPassword());
        options.put("schema-name", getSchemaName());
        return options;
    }

    @Test
    public void testBatchSource() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());

        testSourceFromHTable(options);
    }

    @Test
    public void testBatchSourceWithODP() throws Exception {
        Map<String, String> options = getOptions();
        options.put("odp-mode", "true");
        options.put("odp-ip", ODP.getHost());
        options.put("odp-port", String.valueOf(ODP.getRpcPort()));

        testSourceFromHTable(options);
    }

    @Test
    public void testLookupJoin() throws Exception {
        Map<String, String> options = getOptions();
        options.put("url", getSysParameter("obconfig_url"));
        options.put("sys.username", getSysUsername());
        options.put("sys.password", getSysPassword());
        options.put("lookup.cache.enable", "true");
        options.put("lookup.cache.max-rows", "1000");
        options.put("lookup.cache.ttl", "1min");

        testLookupJoinWithHTable(options);
    }

    @Test
    public void testLookupJoinWithODP() throws Exception {
        Map<String, String> options = getOptions();
        options.put("odp-mode", "true");
        options.put("odp-ip", ODP.getHost());
        options.put("odp-port", String.valueOf(ODP.getRpcPort()));
        options.put("lookup.cache.enable", "true");
        options.put("lookup.cache.max-rows", "1000");
        options.put("lookup.cache.ttl", "1min");

        testLookupJoinWithHTable(options);
    }

    private void testSourceFromHTable(Map<String, String> options) throws Exception {
        // 先插入测试数据
        insertTestData();

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inBatchMode().build());

        // 创建 source 表
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_table ("
                        + " rowkey STRING,"
                        + " family1 ROW<q1 INT>,"
                        + " family2 ROW<q2 STRING, q3 INT>"
                        + ") with ("
                        + "  'connector'='obkv-hbase',"
                        + "  'table-name'='htable',"
                        + "  'scan.caching'='1000',"
                        + getOptionsString(options)
                        + ");");

        // 读取数据
        TableResult result = tEnv.executeSql("SELECT * FROM source_table ORDER BY rowkey");

        List<String> actualRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                actualRows.add(row.toString());
            }
        }

        // 验证结果
        // 预期数据: rowkey=1: q1=1, q2=1, q3=1
        //          rowkey=2: q1=null, q2=2, q3=null
        //          rowkey=3: q1=3, q2=null, q3=null
        //          rowkey=4: q1=4, q2=4, q3=null
        assertEquals(4, actualRows.size());

        // 验证每一行的数据
        assertTrue(actualRows.get(0).contains("1"));
        assertTrue(actualRows.get(1).contains("2"));
        assertTrue(actualRows.get(2).contains("3"));
        assertTrue(actualRows.get(3).contains("4"));

        LOG.info("Batch source test passed. Read {} rows from HBase table.", actualRows.size());
    }

    private void testLookupJoinWithHTable(Map<String, String> options) throws Exception {
        // 先插入测试数据
        insertTestData();

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        execEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

        // 创建维表
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE dim_table ("
                        + " rowkey STRING,"
                        + " family1 ROW<q1 INT>,"
                        + " family2 ROW<q2 STRING, q3 INT>,"
                        + " PRIMARY KEY (rowkey) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='obkv-hbase',"
                        + "  'table-name'='htable',"
                        + getOptionsString(options)
                        + ");");

        // 创建事实表（使用 datagen 生成测试数据）
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE fact_table ("
                        + " id INT,"
                        + " lookup_key STRING,"
                        + " proc_time AS PROCTIME()"
                        + ") WITH ("
                        + " 'connector' = 'datagen',"
                        + " 'number-of-rows' = '4',"
                        + " 'fields.id.kind' = 'sequence',"
                        + " 'fields.id.start' = '1',"
                        + " 'fields.id.end' = '4',"
                        + " 'fields.lookup_key.kind' = 'sequence',"
                        + " 'fields.lookup_key.start' = '1',"
                        + " 'fields.lookup_key.end' = '4'"
                        + ");");

        // 执行 lookup join
        TableResult result =
                tEnv.executeSql(
                        "SELECT "
                                + "  f.id,"
                                + "  f.lookup_key,"
                                + "  d.family1.q1 AS q1,"
                                + "  d.family2.q2 AS q2"
                                + " FROM fact_table AS f"
                                + " LEFT JOIN dim_table FOR SYSTEM_TIME AS OF f.proc_time AS d"
                                + " ON f.lookup_key = d.rowkey");

        List<String> actualRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                actualRows.add(row.toString());
            }
        }

        // 验证结果
        assertEquals(4, actualRows.size());

        LOG.info(
                "Lookup join test passed. Joined {} rows with dimension table.", actualRows.size());
    }

    /** 插入测试数据到 HBase 表 */
    private void insertTestData() throws SQLException, InterruptedException {
        try (Connection connection = getJdbcConnection()) {
            // 插入到 family1
            String sql1 = "INSERT INTO `htable$family1` (K, Q, T, V) VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(sql1)) {
                // rowkey=1, q1=1
                ps.setBytes(1, Bytes.toBytes("1"));
                ps.setBytes(2, Bytes.toBytes("q1"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes(1));
                ps.addBatch();

                // rowkey=3, q1=3
                ps.setBytes(1, Bytes.toBytes("3"));
                ps.setBytes(2, Bytes.toBytes("q1"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes(3));
                ps.addBatch();

                // rowkey=4, q1=4
                ps.setBytes(1, Bytes.toBytes("4"));
                ps.setBytes(2, Bytes.toBytes("q1"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes(4));
                ps.addBatch();

                ps.executeBatch();
            }

            // 插入到 family2
            String sql2 = "INSERT INTO `htable$family2` (K, Q, T, V) VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = connection.prepareStatement(sql2)) {
                // rowkey=1, q2=1, q3=1
                ps.setBytes(1, Bytes.toBytes("1"));
                ps.setBytes(2, Bytes.toBytes("q2"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes("1"));
                ps.addBatch();

                ps.setBytes(1, Bytes.toBytes("1"));
                ps.setBytes(2, Bytes.toBytes("q3"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes(1));
                ps.addBatch();

                // rowkey=2, q2=2
                ps.setBytes(1, Bytes.toBytes("2"));
                ps.setBytes(2, Bytes.toBytes("q2"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes("2"));
                ps.addBatch();

                // rowkey=4, q2=4
                ps.setBytes(1, Bytes.toBytes("4"));
                ps.setBytes(2, Bytes.toBytes("q2"));
                ps.setLong(3, System.currentTimeMillis());
                ps.setBytes(4, Bytes.toBytes("4"));
                ps.addBatch();

                ps.executeBatch();
            }
        }

        // 等待数据写入完成
        waitingAndAssertTableCount("htable$family1", 3);
        waitingAndAssertTableCount("htable$family2", 4);

        LOG.info("Test data inserted successfully.");
    }
}
