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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.Arrays;
import java.util.List;

/** Integration tests for OceanBase parallel snapshot read with all supported data types. */
public class OceanBaseMySQLSourceITCase extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLSourceITCase.class);

    @BeforeAll
    public static void setup() {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
    }

    @AfterAll
    public static void tearDown() {
        CONTAINER.stop();
    }

    @Test
    public void testSourceNoPrimaryKeyTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        initialize("sql/mysql/no_pk_source.sql");

        // Create source table — no PK, connector should auto-detect __pk_increment
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_table ("
                        + " name STRING,"
                        + " `value` INT"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='no_pk_source',"
                        + "  'compatible-mode'='MySQL',"
                        + "  'split-size'='3',"
                        + getOptionsString()
                        + ");");

        // Create sink table
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE sink_table ("
                        + " name STRING,"
                        + " `value` INT"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='no_pk_sink',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table").await();

        waitingAndAssertTableCount("no_pk_sink", 10);

        List<String> expected =
                Arrays.asList(
                        "a,1", "b,2", "c,3", "d,4", "e,5", "f,6", "g,7", "h,8", "i,9", "j,10");

        List<String> actual = queryTable("no_pk_sink");
        assertEqualsInAnyOrder(expected, actual);

        dropTables("no_pk_source", "no_pk_sink");
    }

    @Test
    public void testSourceAllTypes() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        initialize("sql/mysql/all_types_source.sql");

        // Create source table using oceanbase connector with parallel read
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_table ("
                        + " `id` INT NOT NULL,"
                        + " col_boolean BOOLEAN,"
                        + " col_tinyint TINYINT,"
                        + " col_smallint SMALLINT,"
                        + " col_int INT,"
                        + " col_bigint BIGINT,"
                        + " col_float FLOAT,"
                        + " col_double DOUBLE,"
                        + " col_decimal DECIMAL(20, 10),"
                        + " col_char CHAR(10),"
                        + " col_varchar VARCHAR(255),"
                        + " col_date DATE,"
                        + " col_time TIME(0),"
                        + " col_timestamp TIMESTAMP(0),"
                        + " col_binary BINARY(16),"
                        + " col_varbinary VARBINARY(255),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='all_types_source',"
                        + "  'compatible-mode'='MySQL',"
                        + "  'split-size'='2',"
                        + getOptionsString()
                        + ");");

        // Create sink table
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE sink_table ("
                        + " `id` INT NOT NULL,"
                        + " col_boolean BOOLEAN,"
                        + " col_tinyint TINYINT,"
                        + " col_smallint SMALLINT,"
                        + " col_int INT,"
                        + " col_bigint BIGINT,"
                        + " col_float FLOAT,"
                        + " col_double DOUBLE,"
                        + " col_decimal DECIMAL(20, 10),"
                        + " col_char CHAR(10),"
                        + " col_varchar VARCHAR(255),"
                        + " col_date DATE,"
                        + " col_time TIME(0),"
                        + " col_timestamp TIMESTAMP(0),"
                        + " col_binary BINARY(16),"
                        + " col_varbinary VARBINARY(255),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='all_types_sink',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table").await();

        waitingAndAssertTableCount("all_types_sink", 3);

        // Query with HEX() for binary columns
        List<String> fields =
                Arrays.asList(
                        "id",
                        "col_boolean",
                        "col_tinyint",
                        "col_smallint",
                        "col_int",
                        "col_bigint",
                        "col_float",
                        "col_double",
                        "col_decimal",
                        "col_char",
                        "col_varchar",
                        "col_date",
                        "col_time",
                        "col_timestamp",
                        "HEX(col_binary)",
                        "HEX(col_varbinary)");

        List<String> expected =
                Arrays.asList(
                        "1,true,127,32767,2147483647,9223372036854775807,3.14,3.14159265358979,12345.6789012345,hello,world,2025-01-15,12:34:56,2025-01-15 10:30:00.0,48656C6C6F0000000000000000000000,466C696E6B",
                        "2,false,-128,-32768,-2147483648,-9223372036854775808,-1.5,-2.718281828,99999.9999999999,test,OceanBase,2000-06-30,23:59:59,2000-12-31 23:59:59.0,4F6365616E4261736500000000000000,4F42",
                        "3,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null");

        List<String> actual = queryTable("all_types_sink", fields);

        assertEqualsInAnyOrder(expected, actual);

        dropTables("all_types_source", "all_types_sink");
    }

    @Test
    public void testSourceWithStringPrimaryKey() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        initialize("sql/mysql/string_pk_source.sql");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_table ("
                        + " code STRING NOT NULL,"
                        + " name STRING,"
                        + " price DECIMAL(10, 2),"
                        + " PRIMARY KEY (code) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='string_pk_source',"
                        + "  'compatible-mode'='MySQL',"
                        + "  'split-size'='3',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE sink_table ("
                        + " code STRING NOT NULL,"
                        + " name STRING,"
                        + " price DECIMAL(10, 2),"
                        + " PRIMARY KEY (code) NOT ENFORCED"
                        + ") with ("
                        + "  'connector'='oceanbase',"
                        + "  'table-name'='string_pk_sink',"
                        + getOptionsString()
                        + ");");

        tEnv.executeSql("INSERT INTO sink_table SELECT * FROM source_table").await();

        waitingAndAssertTableCount("string_pk_sink", 10);

        List<String> expected =
                Arrays.asList(
                        "A001,Alpha,10.50",
                        "B002,Bravo,20.00",
                        "C003,Charlie,30.75",
                        "D004,Delta,40.25",
                        "E005,Echo,50.00",
                        "F006,Foxtrot,60.80",
                        "G007,Golf,70.10",
                        "H008,Hotel,80.90",
                        "I009,India,90.45",
                        "J010,Juliet,100.00");

        List<String> actual = queryTable("string_pk_sink");
        assertEqualsInAnyOrder(expected, actual);

        dropTables("string_pk_source", "string_pk_sink");
    }
}
