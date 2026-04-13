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

import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for OceanBaseSourceReader type conversion. */
public class OceanBaseSourceReaderTest {

    @Test
    public void testCharAndVarcharConversion() {
        OceanBaseSourceReader reader = createReader();

        Object charResult = reader.convertValueForTest("A", logicalType(DataTypes.CHAR(1)));
        Object varcharResult = reader.convertValueForTest("hello", logicalType(DataTypes.STRING()));

        assertInstanceOf(StringData.class, charResult);
        assertInstanceOf(StringData.class, varcharResult);
        assertEquals("A", charResult.toString());
        assertEquals("hello", varcharResult.toString());
    }

    @Test
    public void testBooleanConversion() {
        OceanBaseSourceReader reader = createReader();

        Object trueResult = reader.convertValueForTest(1, logicalType(DataTypes.BOOLEAN()));
        Object falseResult = reader.convertValueForTest(0, logicalType(DataTypes.BOOLEAN()));
        Object directResult = reader.convertValueForTest(true, logicalType(DataTypes.BOOLEAN()));

        assertEquals(true, trueResult);
        assertEquals(false, falseResult);
        assertEquals(true, directResult);
    }

    @Test
    public void testTinyIntConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest((byte) 127, logicalType(DataTypes.TINYINT()));
        assertTrue(result instanceof Byte);
        assertEquals((byte) 127, result);
    }

    @Test
    public void testSmallIntConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result =
                reader.convertValueForTest((short) 32767, logicalType(DataTypes.SMALLINT()));
        assertTrue(result instanceof Short);
        assertEquals((short) 32767, result);
    }

    @Test
    public void testIntegerConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest(12345, logicalType(DataTypes.INT()));
        assertTrue(result instanceof Integer);
        assertEquals(12345, result);
    }

    @Test
    public void testBigIntConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest(123456789L, logicalType(DataTypes.BIGINT()));
        assertTrue(result instanceof Long);
        assertEquals(123456789L, result);
    }

    @Test
    public void testFloatConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest(3.14f, logicalType(DataTypes.FLOAT()));
        assertTrue(result instanceof Float);
        assertEquals(3.14f, (Float) result, 0.001f);
    }

    @Test
    public void testDoubleConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result =
                reader.convertValueForTest(3.14159265358979, logicalType(DataTypes.DOUBLE()));
        assertTrue(result instanceof Double);
        assertEquals(3.14159265358979, result);
    }

    @Test
    public void testDecimalConversion() {
        OceanBaseSourceReader reader = createReader();
        BigDecimal bd = new BigDecimal("12345.1234567890");
        Object result = reader.convertValueForTest(bd, logicalType(DataTypes.DECIMAL(20, 10)));
        assertInstanceOf(DecimalData.class, result);
        assertEquals(0, bd.compareTo(((DecimalData) result).toBigDecimal()));
    }

    @Test
    public void testDateConversionFromSqlDate() {
        OceanBaseSourceReader reader = createReader();
        Date date = Date.valueOf("2025-01-02");
        Object result = reader.convertValueForTest(date, logicalType(DataTypes.DATE()));
        assertTrue(result instanceof Integer);
        assertEquals((int) date.toLocalDate().toEpochDay(), result);
    }

    @Test
    public void testDateConversionFromLocalDateAndString() {
        OceanBaseSourceReader reader = createReader();
        LocalDate date = LocalDate.of(2025, 2, 3);
        Object localDateResult = reader.convertValueForTest(date, logicalType(DataTypes.DATE()));
        Object stringResult =
                reader.convertValueForTest("2025-02-03", logicalType(DataTypes.DATE()));
        assertEquals((int) date.toEpochDay(), localDateResult);
        assertEquals((int) date.toEpochDay(), stringResult);
    }

    @Test
    public void testTimeConversionFromSqlTime() {
        OceanBaseSourceReader reader = createReader();
        Time time = Time.valueOf("12:34:56");
        Object result = reader.convertValueForTest(time, logicalType(DataTypes.TIME(0)));
        assertEquals(45296000, result);
    }

    @Test
    public void testTimeConversionFromLocalTime() {
        OceanBaseSourceReader reader = createReader();
        LocalTime time = LocalTime.of(12, 34, 56);
        Object result = reader.convertValueForTest(time, logicalType(DataTypes.TIME(0)));
        assertEquals(45296000, result);
    }

    @Test
    public void testTimestampConversion() {
        OceanBaseSourceReader reader = createReader();
        Timestamp ts = Timestamp.valueOf("2025-01-01 10:00:00");
        Object timestampResult =
                reader.convertValueForTest(ts, logicalType(DataTypes.TIMESTAMP(3)));
        Object ldtResult =
                reader.convertValueForTest(
                        LocalDateTime.of(2025, 1, 1, 10, 0, 0),
                        logicalType(DataTypes.TIMESTAMP(3)));

        assertInstanceOf(TimestampData.class, timestampResult);
        assertInstanceOf(TimestampData.class, ldtResult);
    }

    @Test
    public void testTimestampConversionFromStringAndFallback() {
        OceanBaseSourceReader reader = createReader();
        String value = "2025-01-01 10:00:00.0";
        Object result = reader.convertValueForTest(value, logicalType(DataTypes.TIMESTAMP(3)));
        Object fallback = reader.convertValueForTest("bad-ts", logicalType(DataTypes.TIMESTAMP(3)));
        assertInstanceOf(TimestampData.class, result);
        assertEquals("bad-ts", fallback);
    }

    @Test
    public void testTimestampLtzConversion() {
        OceanBaseSourceReader reader = createReader();
        Timestamp ts = Timestamp.valueOf("2025-01-01 10:00:00");
        Object result = reader.convertValueForTest(ts, logicalType(DataTypes.TIMESTAMP_LTZ(3)));
        assertInstanceOf(TimestampData.class, result);
    }

    @Test
    public void testBinaryAndVarbinaryConversion() {
        OceanBaseSourceReader reader = createReader();
        byte[] bytes = new byte[] {1, 2, 3};
        Object binaryResult = reader.convertValueForTest(bytes, logicalType(DataTypes.BINARY(3)));
        Object varbinaryResult =
                reader.convertValueForTest("abc", logicalType(DataTypes.VARBINARY(16)));

        assertInstanceOf(byte[].class, binaryResult);
        assertInstanceOf(byte[].class, varbinaryResult);
        assertArrayEquals(bytes, (byte[]) binaryResult);
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), (byte[]) varbinaryResult);
    }

    @Test
    public void testNullValueConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest(null, logicalType(DataTypes.INT()));
        assertNull(result);
    }

    @Test
    public void testNumberToIntegerConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result =
                reader.convertValueForTest(100L, logicalType(DataTypes.INT())); // JDBC returns Long
        assertTrue(result instanceof Integer);
        assertEquals(100, result);
    }

    @Test
    public void testNumberToSmallIntConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest(100L, logicalType(DataTypes.SMALLINT()));
        assertTrue(result instanceof Short);
        assertEquals((short) 100, result);
    }

    @Test
    public void testNumberToTinyIntConversion() {
        OceanBaseSourceReader reader = createReader();
        Object result = reader.convertValueForTest(100L, logicalType(DataTypes.TINYINT()));
        assertTrue(result instanceof Byte);
        assertEquals((byte) 100, result);
    }

    @Test
    public void testEndOfInputAfterNoMoreSplits() throws Exception {
        OceanBaseSourceReader reader = createReader();
        reader.notifyNoMoreSplits();
        assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(null));
    }

    @Test
    public void testNothingAvailableBeforeNoMoreSplits() throws Exception {
        OceanBaseSourceReader reader = createReader();
        assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(null));
    }

    private OceanBaseSourceReader createReader() {
        OceanBaseSourceConfig config =
                new OceanBaseSourceConfig(
                        "jdbc:oceanbase://127.0.0.1:2881/test",
                        "user",
                        "pwd",
                        "test_db",
                        "products",
                        "MySQL",
                        1024,
                        "id",
                        128);
        return new OceanBaseSourceReader(
                null, config, DataTypes.ROW(DataTypes.FIELD("f", DataTypes.STRING())));
    }

    private LogicalType logicalType(org.apache.flink.table.types.DataType dataType) {
        return dataType.getLogicalType();
    }
}
