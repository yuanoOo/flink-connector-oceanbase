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
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Split enumerator for OceanBase parallel snapshot read. */
public class OceanBaseSplitEnumerator
        implements SplitEnumerator<OceanBaseSplit, OceanBaseEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSplitEnumerator.class);

    private final SplitEnumeratorContext<OceanBaseSplit> context;
    private final OceanBaseSourceConfig config;
    private final ArrayDeque<OceanBaseSplit> pendingSplits;
    private final Map<Integer, OceanBaseSplit> inFlightSplits;
    private final Set<Integer> readersAwaitingSplit;
    private volatile long cachedRowCount = -1L;

    private DruidDataSource dataSource;

    public OceanBaseSplitEnumerator(
            SplitEnumeratorContext<OceanBaseSplit> context,
            OceanBaseSourceConfig config,
            OceanBaseEnumeratorState restoredState) {
        this.context = context;
        this.config = config;
        this.pendingSplits = new ArrayDeque<>();
        this.inFlightSplits = new ConcurrentHashMap<>();
        this.readersAwaitingSplit = ConcurrentHashMap.newKeySet();

        if (restoredState != null) {
            this.pendingSplits.addAll(restoredState.getPendingSplits());
            this.pendingSplits.addAll(restoredState.getInFlightSplits());
            dedupePendingSplits();
        }
    }

    @Override
    public void start() {
        LOG.info("Starting OceanBase split enumerator");

        if (pendingSplits.isEmpty()) {
            discoverSplits();
        }

        assignPendingSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOG.debug("Received split request from subtask {}", subtaskId);
        OceanBaseSplit finishedSplit = inFlightSplits.remove(subtaskId);
        if (finishedSplit != null) {
            LOG.debug(
                    "Reader {} requested new split and finished {}",
                    subtaskId,
                    finishedSplit.splitId());
        }

        if (!pendingSplits.isEmpty()) {
            assignSplitToReader(subtaskId);
        } else {
            readersAwaitingSplit.remove(subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<OceanBaseSplit> splits, int subtaskId) {
        LOG.debug("Received {} splits back from subtask {}", splits.size(), subtaskId);
        inFlightSplits.remove(subtaskId);
        for (OceanBaseSplit split : splits) {
            pendingSplits.addLast(split);
        }
        dedupePendingSplits();
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Reader {} registered", subtaskId);
        readersAwaitingSplit.add(subtaskId);
        inFlightSplits.remove(subtaskId);

        if (!pendingSplits.isEmpty()) {
            assignSplitToReader(subtaskId);
        } else if (pendingSplits.isEmpty() && inFlightSplits.isEmpty()) {
            readersAwaitingSplit.remove(subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public OceanBaseEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new OceanBaseEnumeratorState(
                new ArrayList<>(inFlightSplits.values()), new ArrayList<>(pendingSplits));
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
            for (OceanBaseSplit split : splits) {
                pendingSplits.addLast(split);
            }
            dedupePendingSplits();
            LOG.info(
                    "Discovered {} splits for table {}.{}",
                    pendingSplits.size(),
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
                            + "JOIN all_cons_columns cols ON cons.owner = cols.owner AND cons.constraint_name = cols.constraint_name "
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
            splits.add(
                    new OceanBaseSplit(
                            "0", config.getSchemaName(), config.getTableName(), null, null, null));
            return splits;
        }

        Object[] minMax = getMinMax(splitColumn);
        if (minMax == null) {
            return splits;
        }

        Object min = minMax[0];
        Object max = minMax[1];

        if (min == null || max == null) {
            return splits;
        }

        long rowCount = getRowCountCached();
        int numSplits = Math.max(1, (int) Math.ceil((double) rowCount / config.getSplitSize()));
        numSplits = Math.min(numSplits, Math.max(1, context.currentParallelism()));

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

        List<Object> splitPoints = generateSplitPoints(splitColumn, min, max, numSplits);

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

    private long getRowCountCached() {
        if (cachedRowCount >= 0L) {
            return cachedRowCount;
        }

        try {
            cachedRowCount = getRowCount();
            return cachedRowCount;
        } catch (SQLException e) {
            LOG.warn("Failed to get row count for split generation, fallback to 0", e);
            return 0L;
        }
    }

    private List<Object> generateSplitPoints(
            String splitColumn, Object min, Object max, int numSplits) {
        List<Object> points = new ArrayList<>();
        points.add(null);

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
            BigDecimal step =
                    maxVal.subtract(minVal)
                            .divide(new BigDecimal(numSplits), 16, RoundingMode.HALF_UP);

            for (int i = 1; i < numSplits; i++) {
                points.add(minVal.add(step.multiply(new BigDecimal(i))));
            }
        } else {
            long rowCount = getRowCountCached();
            for (int i = 1; i < numSplits; i++) {
                long offset = Math.max(0L, Math.round((double) rowCount * i / numSplits));
                if (offset <= 0 || offset >= rowCount) {
                    continue;
                }
                Object point = querySplitPointByOffset(splitColumn, offset);
                if (point != null) {
                    points.add(point);
                }
            }
        }

        points.add(null);
        return dedupeAndTrimSplitPoints(points);
    }

    private Object querySplitPointByOffset(String splitColumn, long offset) {
        String quotedSplitColumn = quoteIdentifier(splitColumn);
        String quotedTable =
                quoteIdentifier(config.getSchemaName())
                        + "."
                        + quoteIdentifier(config.getTableName());

        String sql;
        if (config.isOracleMode()) {
            sql =
                    "SELECT val FROM (SELECT val, ROWNUM rn FROM (SELECT "
                            + quotedSplitColumn
                            + " AS val FROM "
                            + quotedTable
                            + " ORDER BY "
                            + quotedSplitColumn
                            + ") WHERE ROWNUM <= ?) WHERE rn = ?";
        } else {
            sql =
                    "SELECT "
                            + quotedSplitColumn
                            + " FROM "
                            + quotedTable
                            + " ORDER BY "
                            + quotedSplitColumn
                            + " LIMIT 1 OFFSET ?";
        }

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            long rowNumber = offset + 1;
            if (config.isOracleMode()) {
                stmt.setLong(1, rowNumber);
                stmt.setLong(2, rowNumber);
            } else {
                stmt.setLong(1, offset);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to query split boundary at offset {}", offset, e);
        }
        return null;
    }

    private List<Object> dedupeAndTrimSplitPoints(List<Object> points) {
        LinkedHashSet<Object> deduped = new LinkedHashSet<>();
        for (Object point : points) {
            deduped.add(point);
        }
        List<Object> sorted = new ArrayList<>(deduped);
        if (sorted.isEmpty()) {
            sorted.add(null);
            sorted.add(null);
            return sorted;
        }
        if (sorted.get(0) != null) {
            sorted.add(0, null);
        }
        if (sorted.get(sorted.size() - 1) != null) {
            sorted.add(null);
        }
        return sorted;
    }

    private String quoteIdentifier(String identifier) {
        if (config.isOracleMode()) {
            return "\"" + identifier + "\"";
        } else {
            return "`" + identifier + "`";
        }
    }

    private void assignPendingSplits() {
        for (int reader : readersAwaitingSplit) {
            if (!pendingSplits.isEmpty() && !inFlightSplits.containsKey(reader)) {
                assignSplitToReader(reader);
            }
        }
    }

    private void assignSplitToReader(int subtaskId) {
        if (inFlightSplits.containsKey(subtaskId)) {
            return;
        }

        if (pendingSplits.isEmpty()) {
            readersAwaitingSplit.remove(subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }

        OceanBaseSplit split = pendingSplits.pollFirst();
        inFlightSplits.put(subtaskId, split);
        Map<Integer, List<OceanBaseSplit>> assignment = new HashMap<>();
        assignment.put(subtaskId, Collections.singletonList(split));
        readersAwaitingSplit.remove(subtaskId);

        context.assignSplits(new SplitsAssignment<>(assignment));
        LOG.debug("Assigned split {} to subtask {}", split.splitId(), subtaskId);
    }

    private void dedupePendingSplits() {
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        ArrayDeque<OceanBaseSplit> deduped = new ArrayDeque<>();
        for (OceanBaseSplit split : pendingSplits) {
            String key =
                    split.getSchemaName()
                            + "."
                            + split.getTableName()
                            + "#"
                            + split.getSplitColumn()
                            + "#"
                            + split.splitId()
                            + "#"
                            + split.getSplitStart()
                            + "#"
                            + split.getSplitEnd();
            if (seen.add(key)) {
                deduped.add(split);
            }
        }
        pendingSplits.clear();
        pendingSplits.addAll(deduped);
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
