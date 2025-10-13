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

package com.oceanbase.connector.flink.table;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.math.BigDecimal;

/** Deserializer to convert HBase Result to Flink RowData. */
public class OBKVHBaseRowDataDeserializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final HTableInfo tableInfo;

    public OBKVHBaseRowDataDeserializer(HTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public RowData deserialize(Result result) {
        if (result == null || result.isEmpty()) {
            return null;
        }

        // Get total field count: rowkey + families
        int fieldCount = 1 + tableInfo.getFamilyNames().size();
        GenericRowData rowData = new GenericRowData(fieldCount);

        // Set rowkey field
        byte[] rowKeyBytes = result.getRow();
        int rowKeyIndex = tableInfo.getFieldIndex(tableInfo.getRowKeyName());
        rowData.setField(rowKeyIndex, deserializeRowKey(rowKeyBytes, tableInfo.getRowKeyType()));

        // Set family fields
        for (String familyName : tableInfo.getFamilyNames()) {
            int familyIndex = tableInfo.getFieldIndex(familyName);
            GenericRowData familyRow = deserializeFamily(result, familyName);
            rowData.setField(familyIndex, familyRow);
        }

        return rowData;
    }

    private Object deserializeRowKey(byte[] rowKeyBytes, LogicalType type) {
        if (rowKeyBytes == null || rowKeyBytes.length == 0) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return StringData.fromString(Bytes.toString(rowKeyBytes));
            case BOOLEAN:
                return Bytes.toBoolean(rowKeyBytes);
            case TINYINT:
                return rowKeyBytes[0];
            case SMALLINT:
                return Bytes.toShort(rowKeyBytes);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return Bytes.toInt(rowKeyBytes);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Bytes.toLong(rowKeyBytes);
            case FLOAT:
                return Bytes.toFloat(rowKeyBytes);
            case DOUBLE:
                return Bytes.toDouble(rowKeyBytes);
            case BINARY:
            case VARBINARY:
                return rowKeyBytes;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported rowkey type: " + type.getTypeRoot());
        }
    }

    private GenericRowData deserializeFamily(Result result, String familyName) {
        String[] columnNames = tableInfo.getColumnNames(familyName);
        LogicalType[] columnTypes = tableInfo.getColumnTypes(familyName);

        GenericRowData familyRow = new GenericRowData(columnNames.length);
        byte[] familyBytes = Bytes.toBytes(familyName);

        for (int i = 0; i < columnNames.length; i++) {
            byte[] qualifierBytes = Bytes.toBytes(columnNames[i]);
            byte[] valueBytes = result.getValue(familyBytes, qualifierBytes);
            Object value = deserializeColumn(valueBytes, columnTypes[i]);
            familyRow.setField(i, value);
        }

        return familyRow;
    }

    private Object deserializeColumn(byte[] bytes, LogicalType type) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return StringData.fromString(Bytes.toString(bytes));
            case BOOLEAN:
                return Bytes.toBoolean(bytes);
            case TINYINT:
                return bytes[0];
            case SMALLINT:
                return Bytes.toShort(bytes);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return Bytes.toInt(bytes);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Bytes.toLong(bytes);
            case FLOAT:
                return Bytes.toFloat(bytes);
            case DOUBLE:
                return Bytes.toDouble(bytes);
            case BINARY:
            case VARBINARY:
                return bytes;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                long milliseconds = Bytes.toLong(bytes);
                return TimestampData.fromEpochMillis(milliseconds);
            case DECIMAL:
                BigDecimal decimal = Bytes.toBigDecimal(bytes);
                return DecimalData.fromBigDecimal(decimal, type.asSummaryString().length(), 0);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported column type: " + type.getTypeRoot());
        }
    }
}
