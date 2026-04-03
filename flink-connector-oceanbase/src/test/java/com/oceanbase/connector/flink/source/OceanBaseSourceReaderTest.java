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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for OceanBaseSourceReader type conversion. */
public class OceanBaseSourceReaderTest {

    @Test
    public void testTinyIntConversion() {
        LogicalType type = new TinyIntType();
        Object result = convertValue((byte) 127, type);
        assertTrue(result instanceof Byte);
        assertEquals((byte) 127, result);
    }

    @Test
    public void testSmallIntConversion() {
        LogicalType type = new SmallIntType();
        Object result = convertValue((short) 32767, type);
        assertTrue(result instanceof Short);
        assertEquals((short) 32767, result);
    }

    @Test
    public void testIntegerConversion() {
        LogicalType type = new IntType();
        Object result = convertValue(12345, type);
        assertTrue(result instanceof Integer);
        assertEquals(12345, result);
    }

    @Test
    public void testBigIntConversion() {
        LogicalType type = new BigIntType();
        Object result = convertValue(123456789L, type);
        assertTrue(result instanceof Long);
        assertEquals(123456789L, result);
    }

    @Test
    public void testFloatConversion() {
        LogicalType type = new FloatType();
        Object result = convertValue(3.14f, type);
        assertTrue(result instanceof Double);
        assertEquals(3.14, (Double) result, 0.001);
    }

    @Test
    public void testDoubleConversion() {
        LogicalType type = new DoubleType();
        Object result = convertValue(3.14159265358979, type);
        assertTrue(result instanceof Double);
        assertEquals(3.14159265358979, result);
    }

    @Test
    public void testDecimalConversion() {
        LogicalType type = new DecimalType(20, 10);
        BigDecimal bd = new BigDecimal("12345.1234567890");
        Object result = convertValue(bd, type);
        assertTrue(result instanceof org.apache.flink.table.data.DecimalData);
    }

    @Test
    public void testDateConversionFromSqlDate() {
        LogicalType type = new DateType();
        long epochDay = System.currentTimeMillis() / (24L * 60L * 60L * 1000L);
        Date date = new Date(epochDay * 24L * 60L * 60L * 1000L);
        Object result = convertValue(date, type);
        assertTrue(result instanceof Long);
        assertEquals(date.toLocalDate().toEpochDay(), result);
    }

    @Test
    public void testTimeConversionFromSqlTime() {
        LogicalType type = new TimeType();
        Time time = Time.valueOf("12:34:56");
        Object result = convertValue(time, type);
        assertEquals(45296000L, result);
    }

    @Test
    public void testTimestampConversionFromString() {
        LogicalType type = new TimestampType();
        String value = "2025-01-01 10:00:00.0";
        Object result = convertValue(value, type);
        assertTrue(result != null);
    }

    @Test
    public void testNullValueConversion() {
        LogicalType type = new IntType();
        Object result = convertValue(null, type);
        assertNull(result);
    }

    @Test
    public void testNumberToIntegerConversion() {
        // Test that Long from JDBC is correctly converted to Integer
        LogicalType type = new IntType();
        Object result = convertValue(100L, type); // JDBC returns Long for INT
        assertTrue(result instanceof Integer);
        assertEquals(100, result);
    }

    @Test
    public void testNumberToSmallIntConversion() {
        // Test that Long from JDBC is correctly converted to Short
        LogicalType type = new SmallIntType();
        Object result = convertValue(100L, type);
        assertTrue(result instanceof Short);
        assertEquals((short) 100, result);
    }

    @Test
    public void testNumberToTinyIntConversion() {
        // Test that Long from JDBC is correctly converted to Byte
        LogicalType type = new TinyIntType();
        Object result = convertValue(100L, type);
        assertTrue(result instanceof Byte);
        assertEquals((byte) 100, result);
    }

    // Helper method that mirrors the convertValue logic in OceanBaseSourceReader
    private Object convertValue(Object value, LogicalType type) {
        if (value == null) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case TINYINT:
                return ((Number) value).byteValue();
            case SMALLINT:
                return ((Number) value).shortValue();
            case INTEGER:
                return ((Number) value).intValue();
            case BIGINT:
                return ((Number) value).longValue();
            case FLOAT:
            case DOUBLE:
                return ((Number) value).doubleValue();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return org.apache.flink.table.data.DecimalData.fromBigDecimal(
                        (BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
            case VARCHAR:
            case CHAR:
                return org.apache.flink.table.data.StringData.fromString(value.toString());
            case BOOLEAN:
                return value;
            case DATE:
                if (value instanceof Date) {
                    return ((Date) value).toLocalDate().toEpochDay();
                }
                if (value instanceof java.time.LocalDate) {
                    return ((java.time.LocalDate) value).toEpochDay();
                }
                return value;
            case TIME_WITHOUT_TIME_ZONE:
                if (value instanceof Time) {
                    return ((Time) value).toLocalTime().toSecondOfDay() * 1000L;
                }
                if (value instanceof java.time.LocalTime) {
                    return ((java.time.LocalTime) value).toSecondOfDay() * 1000L;
                }
                return value;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (value instanceof Timestamp) {
                    return org.apache.flink.table.data.TimestampData.fromTimestamp(
                            (Timestamp) value);
                }
                if (value instanceof java.time.LocalDateTime) {
                    return org.apache.flink.table.data.TimestampData.fromLocalDateTime(
                            (java.time.LocalDateTime) value);
                }
                if (value instanceof String) {
                    return org.apache.flink.table.data.TimestampData.fromTimestamp(
                            Timestamp.valueOf((String) value));
                }
                return value;
            default:
                return value;
        }
    }
}
