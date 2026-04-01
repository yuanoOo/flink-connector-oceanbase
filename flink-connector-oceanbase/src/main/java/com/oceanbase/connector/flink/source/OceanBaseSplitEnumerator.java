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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Split enumerator for OceanBase parallel snapshot read. */
public class OceanBaseSplitEnumerator
        implements SplitEnumerator<OceanBaseSplit, OceanBaseEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSplitEnumerator.class);

    private final SplitEnumeratorContext<OceanBaseSplit> context;
    private final OceanBaseSourceConfig config;
    private final Map<Integer, List<OceanBaseSplit>> pendingSplitsAssignment;
    private final List<OceanBaseSplit> pendingSplits;
    private final List<OceanBaseSplit> assignedSplits;

    private DruidDataSource dataSource;

    public OceanBaseSplitEnumerator(
            SplitEnumeratorContext<OceanBaseSplit> context,
            OceanBaseSourceConfig config,
            OceanBaseEnumeratorState restoredState) {
        this.context = context;
        this.config = config;
        this.pendingSplitsAssignment = new HashMap<>();
        this.pendingSplits = new ArrayList<>();
        this.assignedSplits = new ArrayList<>();

        if (restoredState != null) {
            this.pendingSplits.addAll(restoredState.getPendingSplits());
            this.assignedSplits.addAll(restoredState.getAssignedSplits());
        }
    }

    @Override
    public void start() {
        LOG.info("Starting OceanBase split enumerator");

        if (pendingSplits.isEmpty()) {
            discoverSplits();
        }

        assignSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!pendingSplits.isEmpty()) {
            assignSplits();
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<OceanBaseSplit> splits, int subtaskId) {
        pendingSplits.addAll(splits);
        assignedSplits.removeAll(splits);
        assignSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        // Reader registered, will assign splits on request
    }

    @Override
    public OceanBaseEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new OceanBaseEnumeratorState(
                new ArrayList<>(assignedSplits), new ArrayList<>(pendingSplits));
    }

    @Override
    public void close() throws IOException {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private void discoverSplits() {
        try {
            String splitColumn = config.getChunkKeyColumn();
            if (splitColumn == null || splitColumn.isEmpty()) {
                splitColumn = getDefaultSplitColumn();
            }

            List<OceanBaseSplit> splits = calculateSplits(splitColumn);
            pendingSplits.addAll(splits);
            LOG.info(
                    "Discovered {} splits for table {}.{}",
                    splits.size(),
                    config.getSchemaName(),
                    config.getTableName());

        } catch (SQLException e) {
            LOG.error("Failed to discover splits", e);
            throw new RuntimeException("Failed to discover splits", e);
        }
    }

    private String getDefaultSplitColumn() throws SQLException {
        if (config.isOracleMode()) {
            return "ROWID";
        } else {
            return getPrimaryKeyColumn();
        }
    }

    private String getPrimaryKeyColumn() throws SQLException {
        String sql;
        if (config.isOracleMode()) {
            sql =
                    "SELECT cols.column_name FROM all_constraints cons "
                            + "JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name "
                            + "WHERE cons.owner = ? AND cons.table_name = ? AND cons.constraint_type = 'P'";
        } else {
            sql =
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                            + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' "
                            + "ORDER BY ORDINAL_POSITION LIMIT 1";
        }

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, config.getSchemaName());
            stmt.setString(2, config.getTableName());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }

        LOG.warn(
                "No primary key found for table {}.{}, will use single split",
                config.getSchemaName(),
                config.getTableName());
        return null;
    }

    private List<OceanBaseSplit> calculateSplits(String splitColumn) throws SQLException {
        List<OceanBaseSplit> splits = new ArrayList<>();

        if (splitColumn == null) {
            // No split column, single split
            splits.add(
                    new OceanBaseSplit(
                            "0", config.getSchemaName(), config.getTableName(), null, null, null));
            return splits;
        }

        // Get min and max values of split column
        Object[] minMax = getMinMax(splitColumn);
        if (minMax == null) {
            // Empty table
            return splits;
        }

        Object min = minMax[0];
        Object max = minMax[1];

        if (min == null || max == null) {
            // No data, return empty
            return splits;
        }

        // Calculate split points based on split size
        long rowCount = getRowCount();
        int numSplits = Math.max(1, (int) (rowCount / config.getSplitSize()));
        numSplits = Math.min(numSplits, context.currentParallelism());

        if (numSplits <= 1) {
            splits.add(
                    new OceanBaseSplit(
                            "0",
                            config.getSchemaName(),
                            config.getTableName(),
                            splitColumn,
                            null,
                            null));
            return splits;
        }

        // Generate splits
        List<Object> splitPoints = generateSplitPoints(min, max, numSplits);

        for (int i = 0; i < splitPoints.size() - 1; i++) {
            splits.add(
                    new OceanBaseSplit(
                            String.valueOf(i),
                            config.getSchemaName(),
                            config.getTableName(),
                            splitColumn,
                            splitPoints.get(i),
                            splitPoints.get(i + 1)));
        }

        return splits;
    }

    private Object[] getMinMax(String splitColumn) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT MIN(").append(quoteIdentifier(splitColumn)).append("), ");
        sql.append("MAX(").append(quoteIdentifier(splitColumn)).append(") FROM ");
        sql.append(quoteIdentifier(config.getSchemaName())).append(".");
        sql.append(quoteIdentifier(config.getTableName()));

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString());
                ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return new Object[] {rs.getObject(1), rs.getObject(2)};
            }
        }
        return null;
    }

    private long getRowCount() throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(*) FROM ");
        sql.append(quoteIdentifier(config.getSchemaName())).append(".");
        sql.append(quoteIdentifier(config.getTableName()));

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString());
                ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    private List<Object> generateSplitPoints(Object min, Object max, int numSplits) {
        List<Object> points = new ArrayList<>();
        points.add(null); // First split starts from null (inclusive from beginning)

        if (min instanceof Number && max instanceof Number) {
            double minVal = ((Number) min).doubleValue();
            double maxVal = ((Number) max).doubleValue();
            double step = (maxVal - minVal) / numSplits;

            for (int i = 1; i < numSplits; i++) {
                points.add(minVal + step * i);
            }
        } else if (min instanceof BigDecimal && max instanceof BigDecimal) {
            BigDecimal minVal = (BigDecimal) min;
            BigDecimal maxVal = (BigDecimal) max;
            BigDecimal step = maxVal.subtract(minVal).divide(new BigDecimal(numSplits));

            for (int i = 1; i < numSplits; i++) {
                points.add(minVal.add(step.multiply(new BigDecimal(i))));
            }
        } else {
            // For string types (including ROWID), use string range
            String minStr = min.toString();
            String maxStr = max.toString();

            // For strings, we can't easily split, so just create range-based splits
            // This is a simplified approach
            for (int i = 1; i < numSplits; i++) {
                points.add(minStr);
            }
        }

        points.add(null); // Last split ends at null (inclusive to end)
        return points;
    }

    private String quoteIdentifier(String identifier) {
        if (config.isOracleMode()) {
            return "\"" + identifier + "\"";
        } else {
            return "`" + identifier + "`";
        }
    }

    private void assignSplits() {
        if (pendingSplits.isEmpty()) {
            return;
        }

        Map<Integer, List<OceanBaseSplit>> assignment = new HashMap<>();
        int parallelism = context.currentParallelism();

        for (int i = 0; i < parallelism && !pendingSplits.isEmpty(); i++) {
            List<OceanBaseSplit> splitsForSubtask =
                    assignment.computeIfAbsent(i, k -> new ArrayList<>());
            splitsForSubtask.add(pendingSplits.remove(0));
            assignedSplits.addAll(splitsForSubtask);
        }

        context.assignSplits(new SplitsAssignment<>(assignment));
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
                    dataSource.setMaxActive(5);
                    dataSource.setMaxWait(30000);
                }
            }
        }
        return dataSource;
    }
}
