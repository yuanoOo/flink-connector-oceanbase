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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Source reader for OceanBase parallel snapshot read. */
public class OceanBaseSourceReader implements SourceReader<RowData, OceanBaseSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSourceReader.class);

    private final SourceReaderContext context;
    private final OceanBaseSourceConfig config;
    private final DataType producedDataType;
    private final LogicalType[] fieldTypes;
    private final Deque<OceanBaseSplit> splits = new ArrayDeque<>();
    private DruidDataSource dataSource;
    private volatile boolean running = true;
    private volatile CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

    public OceanBaseSourceReader(
            SourceReaderContext context, OceanBaseSourceConfig config, DataType producedDataType) {
        this.context = context;
        this.config = config;
        this.producedDataType = producedDataType;

        // Extract field types from producedDataType
        List<DataType> children = producedDataType.getChildren();
        this.fieldTypes = new LogicalType[children.size()];
        for (int i = 0; i < children.size(); i++) {
            this.fieldTypes[i] = children.get(i).getLogicalType();
        }
    }

    @Override
    public void start() {
        LOG.info("Starting OceanBase source reader");
    }

    @Override
    public void addSplits(List<OceanBaseSplit> splits) {
        this.splits.addAll(splits);
        // Signal availability when new splits are added
        if (!this.splits.isEmpty()) {
            availabilityFuture.complete(null);
        }
    }

    @Override
    public List<OceanBaseSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("No more splits will be assigned");
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (!splits.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return availabilityFuture;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (splits.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        OceanBaseSplit split = splits.poll();
        if (split == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        try {
            readSplit(split, output);
        } catch (SQLException e) {
            LOG.error("Error reading split: {}", split, e);
            throw new RuntimeException("Failed to read split: " + split.splitId(), e);
        }

        // Check if there are more splits to process
        if (splits.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        return InputStatus.MORE_AVAILABLE;
    }

    private void readSplit(OceanBaseSplit split, ReaderOutput<RowData> output) throws SQLException {
        String sql = buildQuerySQL(split);
        LOG.info("Executing query for split {}: {}", split.splitId(), sql);

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt =
                        conn.prepareStatement(
                                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            stmt.setFetchSize(config.getFetchSize());

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RowData row = convertToRowData(rs);
                    output.collect(row);
                }
            }
        }
    }

    private RowData convertToRowData(ResultSet rs) throws SQLException {
        GenericRowData row = new GenericRowData(fieldTypes.length);
        for (int i = 0; i < fieldTypes.length; i++) {
            Object value = rs.getObject(i + 1);
            row.setField(i, convertValue(value, fieldTypes[i]));
        }
        return row;
    }

    private Object convertValue(Object value, LogicalType type) {
        if (value == null) {
            return null;
        }

        LogicalTypeRoot typeRoot = type.getTypeRoot();
        switch (typeRoot) {
            case VARCHAR:
            case CHAR:
                return StringData.fromString(value.toString());
            case BOOLEAN:
                return value;
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
                return DecimalData.fromBigDecimal(
                        (BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (value instanceof Timestamp) {
                    return TimestampData.fromTimestamp((Timestamp) value);
                } else if (value instanceof LocalDateTime) {
                    return TimestampData.fromLocalDateTime((LocalDateTime) value);
                }
                return TimestampData.fromTimestamp(Timestamp.valueOf(value.toString()));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value instanceof Timestamp) {
                    return TimestampData.fromTimestamp((Timestamp) value);
                }
                return TimestampData.fromTimestamp(Timestamp.valueOf(value.toString()));
            case BINARY:
            case VARBINARY:
                if (value instanceof byte[]) {
                    return value;
                }
                return value.toString().getBytes();
            default:
                return value;
        }
    }

    private String buildQuerySQL(OceanBaseSplit split) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");

        if (config.isOracleMode()) {
            sql.append(quoteIdentifier(split.getSchemaName()))
                    .append(".")
                    .append(quoteIdentifier(split.getTableName()));
        } else {
            sql.append(quoteIdentifier(split.getSchemaName()))
                    .append(".")
                    .append(quoteIdentifier(split.getTableName()));
        }

        String splitColumn = split.getSplitColumn();
        if (splitColumn != null) {
            sql.append(" WHERE ");

            if (split.isFirstSplit() && split.isLastSplit()) {
                // Single split, no WHERE clause needed
                sql.delete(sql.length() - 7, sql.length()); // Remove " WHERE "
            } else if (split.isFirstSplit()) {
                sql.append(quoteIdentifier(splitColumn))
                        .append(" < '")
                        .append(escapeValue(split.getSplitEnd()))
                        .append("'");
            } else if (split.isLastSplit()) {
                sql.append(quoteIdentifier(splitColumn))
                        .append(" >= '")
                        .append(escapeValue(split.getSplitStart()))
                        .append("'");
            } else {
                sql.append(quoteIdentifier(splitColumn))
                        .append(" >= '")
                        .append(escapeValue(split.getSplitStart()))
                        .append("' AND ")
                        .append(quoteIdentifier(splitColumn))
                        .append(" < '")
                        .append(escapeValue(split.getSplitEnd()))
                        .append("'");
            }
        }

        return sql.toString();
    }

    private String quoteIdentifier(String identifier) {
        if (config.isOracleMode()) {
            return "\"" + identifier + "\"";
        } else {
            return "`" + identifier + "`";
        }
    }

    private String escapeValue(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString().replace("'", "''");
    }

    private DruidDataSource getDataSource() {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    dataSource = new DruidDataSource();
                    dataSource.setUrl(config.getUrl());
                    dataSource.setUsername(config.getUsername());
                    dataSource.setPassword(config.getPassword());
                    dataSource.setInitialSize(1);
                    dataSource.setMinIdle(1);
                    dataSource.setMaxActive(10);
                    dataSource.setMaxWait(30000);
                }
            }
        }
        return dataSource;
    }
}
