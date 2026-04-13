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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Failover integration tests for OceanBase parallel snapshot source. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
public class OceanBaseSourceFailoverITCase extends OceanBaseSourceTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseSourceFailoverITCase.class);

    private static final int EXPECTED_ROW_COUNT = 30;

    @BeforeAll
    public static void setup() throws Exception {
        CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG)).start();
        MINI_CLUSTER_RESOURCE.before();
    }

    @AfterAll
    public static void tearDown() {
        MINI_CLUSTER_RESOURCE.after();
        CONTAINER.stop();
    }

    @BeforeEach
    public void initTestData() throws Exception {
        initialize("sql/mysql/failover_test.sql");
        TestValuesTableFactory.clearAllData();
    }

    @AfterEach
    public void cleanUp() throws Exception {
        dropTables("failover_products");
        TestValuesTableFactory.clearAllData();
    }

    @Test
    public void testSnapshotReadWithoutFailover() throws Exception {
        testBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.NONE);
    }

    @Test
    public void testTaskManagerFailoverDuringSnapshot() throws Exception {
        testBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.TM);
    }

    @Test
    public void testJobManagerFailoverDuringSnapshot() throws Exception {
        testBoundedSourceWithFailover(DEFAULT_PARALLELISM, FailoverType.JM);
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testBoundedSourceWithFailover(1, FailoverType.TM);
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testBoundedSourceWithFailover(1, FailoverType.JM);
    }

    private void testBoundedSourceWithFailover(int parallelism, FailoverType failoverType)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE source_products ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'oceanbase',"
                        + " 'table-name' = 'failover_products',"
                        + " 'compatible-mode' = 'MySQL',"
                        + " 'split-size' = '3',"
                        + getOptionsString()
                        + ")");

        tEnv.executeSql(
                "CREATE TEMPORARY TABLE sink_products ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10)"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'true'"
                        + ")");

        TableResult result =
                tEnv.executeSql("INSERT INTO sink_products SELECT * FROM source_products");

        if (failoverType != FailoverType.NONE) {
            JobID jobId = result.getJobClient().get().getJobID();
            waitForSinkSize("sink_products", 3);

            triggerFailover(
                    failoverType,
                    jobId,
                    MINI_CLUSTER_RESOURCE.getMiniCluster(),
                    () -> sleepMs(200));
        }

        result.await();

        List<String> results = TestValuesTableFactory.getResults("sink_products");
        assertExactlyOnce(results);
    }

    private void assertExactlyOnce(List<String> results) {
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                results.size(),
                "Expected " + EXPECTED_ROW_COUNT + " rows but got " + results.size());

        Set<String> unique = new HashSet<>(results);
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                unique.size(),
                "Found duplicate rows: expected "
                        + EXPECTED_ROW_COUNT
                        + " unique rows but got "
                        + unique.size());

        for (int i = 1; i <= EXPECTED_ROW_COUNT; i++) {
            String prefix = "+I[" + i + ", ";
            boolean found = results.stream().anyMatch(r -> r.startsWith(prefix));
            Assertions.assertTrue(found, "Missing row with id=" + i);
        }
    }

    private static void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }
}
