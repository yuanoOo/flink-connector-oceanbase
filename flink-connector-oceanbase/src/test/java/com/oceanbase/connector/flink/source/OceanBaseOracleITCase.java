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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for OceanBase Oracle mode parallel snapshot read. Run with environment
 * variables: HOST, PORT, SCHEMA_NAME, USER_NAME, PASSWORD
 */
@Disabled("Requires OceanBase Oracle mode instance")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OceanBaseOracleITCase {

    private String host;
    private int port;
    private String schemaName;
    private String username;
    private String password;

    @BeforeAll
    public void setup() throws Exception {
        host = System.getenv("HOST");
        port = Integer.parseInt(System.getenv().getOrDefault("PORT", "2881"));
        schemaName = System.getenv("SCHEMA_NAME");
        username = System.getenv("USER_NAME");
        password = System.getenv("PASSWORD");

        // Create test table
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE test_parallel_read ("
                            + "id NUMBER PRIMARY KEY, "
                            + "name VARCHAR2(255), "
                            + "value NUMBER)");
            for (int i = 1; i <= 100; i++) {
                stmt.execute(
                        "INSERT INTO test_parallel_read VALUES ("
                                + i
                                + ", 'name_"
                                + i
                                + "', "
                                + (i * 10)
                                + ")");
            }
            stmt.execute("COMMIT");
        }
    }

    @AfterAll
    public void tearDown() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_parallel_read");
        }
    }

    @Test
    public void testConnection() throws Exception {
        try (Connection conn = getConnection()) {
            assertTrue(conn.isValid(5));
        }
    }

    @Test
    public void testQueryWithRowid() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            ResultSet rs =
                    stmt.executeQuery(
                            "SELECT ROWID, id, name FROM test_parallel_read WHERE ROWNUM <= 10");
            int count = 0;
            while (rs.next()) {
                String rowid = rs.getString(1);
                assertTrue(rowid != null && !rowid.isEmpty());
                count++;
            }
            assertEquals(10, count);
        }
    }

    @Test
    public void testRowidRange() throws Exception {
        // Test ROWID range query for parallel splitting
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // Get min and max ROWID
            ResultSet rs =
                    stmt.executeQuery("SELECT MIN(ROWID), MAX(ROWID) FROM test_parallel_read");
            assertTrue(rs.next());
            String minRowid = rs.getString(1);
            String maxRowid = rs.getString(2);
            assertTrue(minRowid != null && !minRowid.isEmpty());
            assertTrue(maxRowid != null && !maxRowid.isEmpty());
        }
    }

    @Test
    public void testDataRead() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_parallel_read");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }
    }

    @Test
    public void testSplitQuery() throws Exception {
        // Test split query with ROWID range
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // Get first 50 rows by ROWID
            ResultSet rs =
                    stmt.executeQuery(
                            "SELECT * FROM test_parallel_read WHERE ROWID IN "
                                    + "(SELECT ROWID FROM test_parallel_read WHERE ROWNUM <= 50)");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(50, count);
        }
    }

    private Connection getConnection() throws Exception {
        String url =
                "jdbc:oceanbase://"
                        + host
                        + ":"
                        + port
                        + "/"
                        + schemaName
                        + "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        return DriverManager.getConnection(url, username, password);
    }
}
