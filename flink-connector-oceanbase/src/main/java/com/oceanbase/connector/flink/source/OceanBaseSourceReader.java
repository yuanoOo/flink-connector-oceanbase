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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
    private final LogicalType[] fieldTypes;
    private final Deque<OceanBaseSplit> pendingSplits = new ArrayDeque<>();
    private OceanBaseSplit currentSplit;
    private DruidDataSource dataSource;
    private volatile boolean running = true;
    private volatile CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

    public OceanBaseSourceReader(
            SourceReaderContext context, OceanBaseSourceConfig config, DataType producedDataType) {
        this.context = context;
        this.config = config;

        List<DataType> children = producedDataType.getChildren();
        this.fieldTypes = new LogicalType[children.size()];
        for (int i = 0; i < children.size(); i++) {
            this.fieldTypes[i] = children.get(i).getLogicalType();
        }
    }

    @Override
    public void start() {
        LOG.info("Starting OceanBase source reader");
        requestSplit();
    }

    @Override
    public void addSplits(List<OceanBaseSplit> splits) {
        synchronized (pendingSplits) {
            pendingSplits.addAll(splits);
        }
        availabilityFuture.complete(null);
        requestSplit();
    }

    @Override
    public List<OceanBaseSplit> snapshotState(long checkpointId) {
        List<OceanBaseSplit> state = new ArrayList<>();
        synchronized (pendingSplits) {
            if (currentSplit != null) {
                state.add(currentSplit);
            }
            state.addAll(pendingSplits);
        }
        return state;
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
        synchronized (pendingSplits) {
            if (currentSplit != null || !pendingSplits.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            if (availabilityFuture.isDone()) {
                availabilityFuture = new CompletableFuture<>();
            }
            return availabilityFuture;
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (!running) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (currentSplit == null) {
            synchronized (pendingSplits) {
                currentSplit = pendingSplits.pollFirst();
            }
        }

        if (currentSplit == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        try {
            readSplit(currentSplit, output);
            currentSplit = null;
            requestSplit();
        } catch (SQLException e) {
            LOG.error("Error reading split: {}", currentSplit, e);
            throw new RuntimeException("Failed to read split: " + currentSplit.splitId(), e);
        }

        synchronized (pendingSplits) {
            return pendingSplits.isEmpty()
                    ? InputStatus.NOTHING_AVAILABLE
                    : InputStatus.MORE_AVAILABLE;
        }
    }

    private void requestSplit() {
        try {
            context.sendSplitRequest();
        } catch (Exception e) {
            LOG.debug("Failed to request more splits", e);
        }
    }

    private void readSplit(OceanBaseSplit split, ReaderOutput<RowData> output) throws SQLException {
        List<Object> params = new ArrayList<>();
        String sql = buildQuerySQL(split, params);
        LOG.info("Executing query for split {}: {}", split.splitId(), sql);

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt =
                        conn.prepareStatement(
                                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
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
                if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
                }
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
                if (value instanceof BigDecimal) {
                    return DecimalData.fromBigDecimal(
                            (BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
                }
                return DecimalData.fromBigDecimal(
                        new BigDecimal(value.toString()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case DATE:
                {
                    if (value instanceof java.sql.Date) {
                        return ((java.sql.Date) value).toLocalDate().toEpochDay();
                    }
                    if (value instanceof LocalDate) {
                        return ((LocalDate) value).toEpochDay();
                    }
                    if (value instanceof String) {
                        return LocalDate.parse((String) value).toEpochDay();
                    }
                    return value;
                }
            case TIME_WITHOUT_TIME_ZONE:
                if (value instanceof java.sql.Time) {
                    return ((java.sql.Time) value).toLocalTime().toSecondOfDay() * 1000;
                }
                if (value instanceof LocalTime) {
                    return ((LocalTime) value).toSecondOfDay() * 1000;
                }
                return value;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value instanceof Timestamp) {
                    return TimestampData.fromTimestamp((Timestamp) value);
                }
                if (value instanceof LocalDateTime) {
                    return TimestampData.fromLocalDateTime((LocalDateTime) value);
                }
                if (value instanceof String) {
                    try {
                        return TimestampData.fromTimestamp(Timestamp.valueOf((String) value));
                    } catch (IllegalArgumentException ignored) {
                        return value;
                    }
                }
                return value;
            case BINARY:
            case VARBINARY:
                if (value instanceof byte[]) {
                    return value;
                }
                if (value instanceof String) {
                    try {
                        return ((String) value).getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        return value.toString().getBytes();
                    }
                }
                return value.toString().getBytes();
            default:
                return value;
        }
    }

    String buildQueryForTest(OceanBaseSplit split) {
        List<Object> params = new ArrayList<>();
        return buildQuerySQL(split, params);
    }

    String[] buildQueryParamsForTest(OceanBaseSplit split) {
        List<Object> params = new ArrayList<>();
        buildQuerySQL(split, params);
        String[] values = new String[params.size()];
        for (int i = 0; i < params.size(); i++) {
            values[i] = String.valueOf(params.get(i));
        }
        return values;
    }

    private String buildQuerySQL(OceanBaseSplit split, List<Object> params) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(quoteIdentifier(split.getSchemaName()))
                .append(".")
                .append(quoteIdentifier(split.getTableName()));

        String splitColumn = split.getSplitColumn();
        if (splitColumn != null) {
            if (split.isFirstSplit() && split.isLastSplit()) {
                return sql.toString();
            }

            sql.append(" WHERE ");
            if (split.isFirstSplit()) {
                sql.append(quoteIdentifier(splitColumn)).append(" < ?");
                params.add(split.getSplitEnd());
            } else if (split.isLastSplit()) {
                sql.append(quoteIdentifier(splitColumn)).append(" >= ?");
                params.add(split.getSplitStart());
            } else {
                sql.append(quoteIdentifier(splitColumn))
                        .append(" >= ? AND ")
                        .append(quoteIdentifier(splitColumn))
                        .append(" < ?");
                params.add(split.getSplitStart());
                params.add(split.getSplitEnd());
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
