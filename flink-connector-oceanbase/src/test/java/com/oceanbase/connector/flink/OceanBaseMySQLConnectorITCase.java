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

import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.OceanBaseTestData;
import com.oceanbase.connector.flink.table.OceanBaseTestDataSerializationSchema;
import com.oceanbase.connector.flink.table.SchemaChangeRecord;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.connector.flink.utils.OceanBaseJdbcUtils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OceanBaseMySQLConnectorITCase extends OceanBaseMySQLTestBase {

    @ClassRule public static final GenericContainer<?> CONTAINER = container("sql/init.sql");

    private static final String TEST_TABLE = "products";

    @Override
    protected Map<String, String> getOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("url", getJdbcUrl(CONTAINER));
        options.put("username", TEST_USERNAME);
        options.put("password", TEST_PASSWORD);
        options.put("schema-name", TEST_DATABASE);
        options.put("table-name", TEST_TABLE);
        options.put("druid-properties", "druid.initialSize=4;druid.maxActive=20;");
        return options;
    }

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(CONTAINER), TEST_USERNAME, TEST_PASSWORD);
    }

    @After
    public void after() throws Exception {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM products");
            statement.execute("DELETE FROM gis_types");
        }
    }

    @Test
    public void testMultipleTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(getOptions());
        OceanBaseSink<OceanBaseTestData> sink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OceanBaseTestDataSerializationSchema(),
                        DataChangeRecord.KeyExtractor.simple(),
                        new OceanBaseRecordFlusher(connectorOptions));

        OceanBaseDialect dialect = new OceanBaseMySQLDialect();
        String database = TEST_DATABASE;
        String tableA = TEST_TABLE + "A";
        String tableB = TEST_TABLE + "B";
        String tableC = TEST_TABLE + "C";

        String tableFullNameA = dialect.getFullTableName(database, tableA);
        String tableFullNameB = dialect.getFullTableName(database, tableB);
        String tableFullNameC = dialect.getFullTableName(database, tableC);

        ResolvedSchema schemaA =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.INT().notNull()),
                                Column.physical("a1", DataTypes.INT().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));
        ResolvedSchema schemaB =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("b", DataTypes.INT().notNull()),
                                Column.physical("b1", DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("b")));
        ResolvedSchema schemaC =
                ResolvedSchema.of(
                        Arrays.asList(
                                Column.physical("c", DataTypes.INT().notNull()),
                                Column.physical("c1", DataTypes.STRING().notNull())));

        // create table and insert rows
        List<OceanBaseTestData> dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.CREATE,
                                String.format(
                                        "CREATE TABLE %s (a int(10) primary key, a1 int(10))",
                                        tableFullNameA)),
                        new OceanBaseTestData(
                                database,
                                tableB,
                                SchemaChangeRecord.Type.CREATE,
                                String.format(
                                        "CREATE TABLE %s (b int(10) primary key, b1 varchar(20))",
                                        tableFullNameB)),
                        new OceanBaseTestData(
                                database,
                                tableC,
                                SchemaChangeRecord.Type.CREATE,
                                String.format(
                                        "CREATE TABLE %s (c int(10), c1 varchar(20))",
                                        tableFullNameC)),
                        new OceanBaseTestData(
                                "test",
                                tableA,
                                schemaA,
                                GenericRowData.ofKind(RowKind.INSERT, 1, 1)),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, 2, StringData.fromString("2"))),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, 3, StringData.fromString("3"))),
                        new OceanBaseTestData(
                                "test",
                                tableC,
                                schemaC,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, 4, StringData.fromString("4"))));

        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        assertEqualsInAnyOrder(queryTable(tableFullNameA), Collections.singletonList("1,1"));
        assertEqualsInAnyOrder(queryTable(tableFullNameB), Arrays.asList("2,2", "3,3"));
        assertEqualsInAnyOrder(queryTable(tableFullNameC), Collections.singletonList("4,4"));

        // update and delete
        dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                "test",
                                tableA,
                                schemaA,
                                GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1, 2)),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER, 2, StringData.fromString("3"))),
                        new OceanBaseTestData(
                                "test",
                                tableB,
                                schemaB,
                                GenericRowData.ofKind(
                                        RowKind.DELETE, 3, StringData.fromString("3"))));

        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        assertEqualsInAnyOrder(queryTable(tableFullNameA), Collections.singletonList("1,2"));
        assertEqualsInAnyOrder(queryTable(tableFullNameB), Collections.singletonList("2,3"));

        // truncate table
        dataSet =
                Arrays.asList(
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.TRUNCATE,
                                String.format("TRUNCATE TABLE %s", tableFullNameA)),
                        new OceanBaseTestData(
                                database,
                                tableA,
                                SchemaChangeRecord.Type.TRUNCATE,
                                String.format("TRUNCATE TABLE %s", tableFullNameB)));
        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        assertTrue(CollectionUtils.isEmpty(queryTable(tableFullNameA)));
        assertTrue(CollectionUtils.isEmpty(queryTable(tableFullNameB)));
    }

    @Test
    public void testDataStreamSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(getOptions());
        ResolvedSchema physicalSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.INT().notNull()),
                                Column.physical("name", DataTypes.STRING().notNull()),
                                Column.physical("description", DataTypes.STRING().notNull()),
                                Column.physical("weight", DataTypes.DECIMAL(20, 10).notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        OceanBaseSink<RowData> sink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OceanBaseRowDataSerializationSchema(
                                new TableInfo(
                                        new TableId(
                                                connectionProvider.getDialect()::getFullTableName,
                                                connectorOptions.getSchemaName(),
                                                connectorOptions.getTableName()),
                                        physicalSchema)),
                        DataChangeRecord.KeyExtractor.simple(),
                        new OceanBaseRecordFlusher(connectorOptions));

        List<RowData> dataSet =
                Arrays.asList(
                        rowData(101, "scooter", "Small 2-wheel scooter", 3.14),
                        rowData(102, "car battery", "12V car battery", 8.1),
                        rowData(
                                103,
                                "12-pack drill bits",
                                "12-pack of drill bits with sizes ranging from #40 to #3",
                                0.8),
                        rowData(104, "hammer", "12oz carpenter's hammer", 0.75),
                        rowData(105, "hammer", "14oz carpenter's hammer", 0.875),
                        rowData(106, "hammer", "16oz carpenter's hammer", 1.0),
                        rowData(107, "rocks", "box of assorted rocks", 5.3),
                        rowData(108, "jacket", "water resistent black wind breaker", 0.1),
                        rowData(109, "spare tire", "24 inch spare tire", 22.2));

        env.fromCollection(dataSet).sinkTo(sink);
        env.execute();

        validateSinkResults();
    }

    private RowData rowData(int id, String name, String description, double weight) {
        return GenericRowData.of(
                id,
                StringData.fromString(name),
                StringData.fromString(description),
                DecimalData.fromBigDecimal(new BigDecimal(weight), 20, 10));
    }

    @Test
    public void testGis() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String tableName = "gis_types";
        Map<String, String> options = getOptions();
        options.put("table-name", tableName);

        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(options);
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        OceanBaseSink<RowData> sink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new OceanBaseRowDataSerializationSchema(
                                new TableInfo(
                                        new TableId(
                                                connectionProvider.getDialect()::getFullTableName,
                                                connectorOptions.getSchemaName(),
                                                connectorOptions.getTableName()),
                                        Collections.singletonList("id"),
                                        Arrays.asList(
                                                "id",
                                                "point_c",
                                                "geometry_c",
                                                "linestring_c",
                                                "polygon_c",
                                                "multipoint_c",
                                                "multiline_c",
                                                "multipolygon_c",
                                                "geometrycollection_c"),
                                        Arrays.asList(
                                                DataTypes.INT().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType(),
                                                DataTypes.STRING().getLogicalType()),
                                        x -> "id".equals(x) ? "?" : "ST_GeomFromText(?)")),
                        DataChangeRecord.KeyExtractor.simple(),
                        new OceanBaseRecordFlusher(connectorOptions));

        List<String> values =
                Arrays.asList(
                        "POINT(1 1)",
                        "POLYGON((1 1,2 1,2 2,1 2,1 1))",
                        "LINESTRING(3 0,3 3,3 5)",
                        "POLYGON((1 1,2 1,2 2,1 2,1 1))",
                        "MULTIPOINT((1 1),(2 2))",
                        "MULTILINESTRING((1 1,2 2,3 3),(4 4,5 5))",
                        "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7,5 5)))",
                        "GEOMETRYCOLLECTION(POINT(10 10),POINT(30 30),LINESTRING(15 15,20 20))");

        GenericRowData rowData = new GenericRowData(RowKind.INSERT, values.size() + 1);
        rowData.setField(0, 1);
        for (int i = 0; i < values.size(); i++) {
            rowData.setField(i + 1, StringData.fromString(values.get(i)));
        }

        env.fromElements((RowData) rowData).sinkTo(sink);
        env.execute();

        waitingAndAssertTableCount(tableName, 1);
        List<String> actual =
                queryTable(
                        tableName,
                        Arrays.asList(
                                "id",
                                "ST_AsWKT(point_c)",
                                "ST_AsWKT(geometry_c)",
                                "ST_AsWKT(linestring_c)",
                                "ST_AsWKT(polygon_c)",
                                "ST_AsWKT(multipoint_c)",
                                "ST_AsWKT(multiline_c)",
                                "ST_AsWKT(multipolygon_c)",
                                "ST_AsWKT(geometrycollection_c)"));

        assertEquals(actual.get(0), "1," + String.join(",", values));
    }

    @Test
    public void testDirectLoadSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'direct-load.enabled'='true',"
                        + "  'direct-load.host'='"
                        + CONTAINER.getHost()
                        + "',"
                        + "  'direct-load.port'='"
                        + CONTAINER.getMappedPort(RPC_PORT)
                        + "',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO target "
                                + "VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14),"
                                + "       (102, 'car battery', '12V car battery', 8.1),"
                                + "       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),"
                                + "       (104, 'hammer', '12oz carpenter''s hammer', 0.75),"
                                + "       (105, 'hammer', '14oz carpenter''s hammer', 0.875),"
                                + "       (106, 'hammer', '16oz carpenter''s hammer', 1.0),"
                                + "       (107, 'rocks', 'box of assorted rocks', 5.3),"
                                + "       (108, 'jacket', 'water resistent black wind breaker', 0.1),"
                                + "       (109, 'spare tire', '24 inch spare tire', 22.2);")
                .await();

        validateSinkResults();
    }

    @Test
    public void testSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE target ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql(
                        "INSERT INTO target "
                                + "VALUES (101, 'scooter', 'Small 2-wheel scooter', 3.14),"
                                + "       (102, 'car battery', '12V car battery', 8.1),"
                                + "       (103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),"
                                + "       (104, 'hammer', '12oz carpenter''s hammer', 0.75),"
                                + "       (105, 'hammer', '14oz carpenter''s hammer', 0.875),"
                                + "       (106, 'hammer', '16oz carpenter''s hammer', 1.0),"
                                + "       (107, 'rocks', 'box of assorted rocks', 5.3),"
                                + "       (108, 'jacket', 'water resistent black wind breaker', 0.1),"
                                + "       (109, 'spare tire', '24 inch spare tire', 22.2);")
                .await();

        validateSinkResults();
    }

    private void validateSinkResults() throws SQLException, InterruptedException {
        List<String> expected =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.1400000000",
                        "102,car battery,12V car battery,8.1000000000",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000",
                        "104,hammer,12oz carpenter's hammer,0.7500000000",
                        "105,hammer,14oz carpenter's hammer,0.8750000000",
                        "106,hammer,16oz carpenter's hammer,1.0000000000",
                        "107,rocks,box of assorted rocks,5.3000000000",
                        "108,jacket,water resistent black wind breaker,0.1000000000",
                        "109,spare tire,24 inch spare tire,22.2000000000");

        waitingAndAssertTableCount(TEST_TABLE, expected.size());

        List<String> actual = queryTable(TEST_TABLE);

        assertEqualsInAnyOrder(expected, actual);
    }

    private void waitingAndAssertTableCount(String tableName, int expectedCount)
            throws InterruptedException {
        int tableRowsCount = 0;
        for (int i = 0; i < 100; ++i) {
            tableRowsCount = OceanBaseJdbcUtils.getTableRowsCount(this::getConnection, tableName);
            if (tableRowsCount < expectedCount) {
                Thread.sleep(100);
            }
        }
        Assert.assertEquals(expectedCount, tableRowsCount);
    }

    public List<String> queryTable(String tableName) throws SQLException {
        return queryTable(tableName, Collections.singletonList("*"));
    }

    public List<String> queryTable(String tableName, List<String> fields) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs =
                    statement.executeQuery(
                            "SELECT " + String.join(", ", fields) + " FROM " + tableName);
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(rs.getObject(i + 1));
                }
                result.add(sb.toString());
            }
        }
        return result;
    }
}
