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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Split enumerator for OceanBase parallel snapshot read.
 *
 * <p>Split discovery runs asynchronously on a background thread. Splits are added to the pending
 * queue incrementally as they are calculated, allowing readers to start processing data before all
 * splits have been discovered. For non-numeric primary keys (e.g., VARCHAR), split boundaries are
 * calculated using an iterative next-chunk-max approach that avoids deep pagination: each query
 * starts from the last known boundary and scans only {@code splitSize} rows forward.
 */
public class OceanBaseSplitEnumerator
        implements SplitEnumerator<OceanBaseSplit, OceanBaseEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSplitEnumerator.class);
    private static final long SPLIT_CHECK_INTERVAL_MS = 200L;

    private final SplitEnumeratorContext<OceanBaseSplit> context;
    private final OceanBaseSourceConfig config;
    private final ArrayDeque<OceanBaseSplit> pendingSplits;
    private final Map<Integer, OceanBaseSplit> inFlightSplits;
    private final Set<Integer> readersAwaitingSplit;
    private volatile long cachedRowCount = -1L;
    private volatile boolean splitDiscoveryFinished = false;

    private volatile DruidDataSource dataSource;
    private ExecutorService splitDiscoveryExecutor;
    // Tracks split boundary keys already in pendingSplits to avoid adding duplicates during
    // re-discovery after checkpoint restore.
    private final Set<String> knownSplitKeys = ConcurrentHashMap.newKeySet();

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
            for (OceanBaseSplit split : pendingSplits) {
                knownSplitKeys.add(splitBoundaryKey(split));
            }
            this.splitDiscoveryFinished = restoredState.isSplitDiscoveryFinished();
        }
    }

    @Override
    public void start() {
        LOG.info("Starting OceanBase split enumerator");

        if (!splitDiscoveryFinished) {
            startAsyncSplitDiscovery();
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

        synchronized (pendingSplits) {
            if (!pendingSplits.isEmpty()) {
                assignSplitToReader(subtaskId);
            } else if (splitDiscoveryFinished && inFlightSplits.isEmpty()) {
                readersAwaitingSplit.remove(subtaskId);
                context.signalNoMoreSplits(subtaskId);
            } else {
                readersAwaitingSplit.add(subtaskId);
            }
        }
    }

    @Override
    public void addSplitsBack(List<OceanBaseSplit> splits, int subtaskId) {
        LOG.debug("Received {} splits back from subtask {}", splits.size(), subtaskId);
        inFlightSplits.remove(subtaskId);
        synchronized (pendingSplits) {
            for (OceanBaseSplit split : splits) {
                pendingSplits.addLast(split);
            }
            dedupePendingSplits();
        }
        assignPendingSplits();

        if (splitDiscoveryFinished) {
            signalNoMoreSplitsIfDone();
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Reader {} registered", subtaskId);
        readersAwaitingSplit.add(subtaskId);
        inFlightSplits.remove(subtaskId);

        synchronized (pendingSplits) {
            if (!pendingSplits.isEmpty()) {
                assignSplitToReader(subtaskId);
            } else if (splitDiscoveryFinished && inFlightSplits.isEmpty()) {
                readersAwaitingSplit.remove(subtaskId);
                context.signalNoMoreSplits(subtaskId);
            }
        }
    }

    @Override
    public OceanBaseEnumeratorState snapshotState(long checkpointId) throws Exception {
        synchronized (pendingSplits) {
            return new OceanBaseEnumeratorState(
                    new ArrayList<>(inFlightSplits.values()),
                    new ArrayList<>(pendingSplits),
                    splitDiscoveryFinished);
        }
    }

    @Override
    public void close() throws IOException {
        if (splitDiscoveryExecutor != null) {
            splitDiscoveryExecutor.shutdownNow();
            try {
                splitDiscoveryExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

    // ---- Async split discovery ----

    private void startAsyncSplitDiscovery() {
        splitDiscoveryExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "ob-split-discovery");
                            t.setDaemon(true);
                            return t;
                        });
        splitDiscoveryExecutor.submit(this::discoverSplitsAsync);

        // Periodically check for newly discovered splits and assign them
        context.callAsync(
                () -> !splitDiscoveryFinished,
                (stillRunning, error) -> {
                    if (error != null) {
                        LOG.error("Error checking split discovery status", error);
                        return;
                    }
                    assignPendingSplits();
                    if (splitDiscoveryFinished) {
                        signalNoMoreSplitsIfDone();
                    }
                },
                0,
                SPLIT_CHECK_INTERVAL_MS);
    }

    private void discoverSplitsAsync() {
        try {
            String splitColumn = config.getChunkKeyColumn();
            if (splitColumn == null || splitColumn.isEmpty()) {
                splitColumn = getDefaultSplitColumn();
            }

            calculateSplitsAsync(splitColumn);

            LOG.info(
                    "Async split discovery completed for table {}.{}",
                    config.getSchemaName(),
                    config.getTableName());
        } catch (SQLException e) {
            LOG.error("Failed to discover splits asynchronously", e);
            throw new RuntimeException("Failed to discover splits", e);
        } finally {
            splitDiscoveryFinished = true;
        }
    }

    private void calculateSplitsAsync(String splitColumn) throws SQLException {
        if (splitColumn == null) {
            addSplitToPending(
                    new OceanBaseSplit(
                            "0", config.getSchemaName(), config.getTableName(), null, null, null));
            return;
        }

        Object[] minMax = getMinMax(splitColumn);
        if (minMax == null) {
            return;
        }

        Object min = minMax[0];
        Object max = minMax[1];

        if (min == null || max == null) {
            return;
        }

        // For numeric types, calculate all split points at once (no DB queries needed)
        if (isNumericSplittable(min, max)) {
            long rowCount = getRowCountCached();
            int numSplits = Math.max(1, (int) Math.ceil((double) rowCount / config.getSplitSize()));

            if (numSplits <= 1) {
                addSplitToPending(
                        new OceanBaseSplit(
                                "0",
                                config.getSchemaName(),
                                config.getTableName(),
                                splitColumn,
                                null,
                                null));
                return;
            }

            List<Object> splitPoints = generateNumericSplitPoints(min, max, numSplits);
            addAllSplitsFromPoints(splitColumn, splitPoints);
            return;
        }

        // For non-numeric types (String, Date, etc.), use iterative next-chunk-max approach.
        // Each query starts from the last boundary and scans only chunkSize rows forward,
        // avoiding the deep pagination problem of LIMIT/OFFSET.
        // Splits are emitted incrementally so readers can start before all points are found.
        generateSplitsIncrementally(splitColumn, min, max);
    }

    private boolean isNumericSplittable(Object min, Object max) {
        return (min instanceof BigDecimal && max instanceof BigDecimal)
                || (min instanceof Long || min instanceof Integer)
                || (min instanceof Number && max instanceof Number);
    }

    /**
     * Generates splits incrementally using the next-chunk-max approach (inspired by flink-cdc).
     *
     * <p>Instead of using LIMIT/OFFSET which suffers from deep pagination (MySQL scans N rows for
     * OFFSET N), this method iteratively queries: {@code SELECT MAX(col) FROM (SELECT col FROM
     * table WHERE col >= ? ORDER BY col LIMIT chunkSize) AS T}
     *
     * <p>Each query always starts from the last known boundary and only scans {@code chunkSize}
     * rows forward, so performance is O(chunkSize) per query regardless of table position.
     */
    private void generateSplitsIncrementally(String splitColumn, Object min, Object max) {
        int splitIndex = 0;
        Object chunkStart = null; // null = open lower bound (first split)
        Object currentLowerBound = min;
        int chunkSize = config.getSplitSize();

        while (currentLowerBound != null) {
            Object nextBound = queryNextChunkMax(splitColumn, chunkSize, currentLowerBound);

            if (nextBound == null) {
                break;
            }

            // Handle duplicate boundaries: many rows share the same split column value
            if (nextBound.equals(currentLowerBound)) {
                nextBound = queryMinGreaterThan(splitColumn, currentLowerBound);
                if (nextBound == null) {
                    // currentLowerBound is the max value, no more distinct values
                    break;
                }
            }

            // Emit split [chunkStart, nextBound)
            addSplitToPending(
                    new OceanBaseSplit(
                            String.valueOf(splitIndex++),
                            config.getSchemaName(),
                            config.getTableName(),
                            splitColumn,
                            chunkStart,
                            nextBound));

            LOG.debug(
                    "Discovered split {} for table {}.{} boundary={}",
                    splitIndex,
                    config.getSchemaName(),
                    config.getTableName(),
                    nextBound);

            chunkStart = nextBound;
            currentLowerBound = nextBound;
        }

        // Final split: [chunkStart, null) — covers remaining rows
        addSplitToPending(
                new OceanBaseSplit(
                        String.valueOf(splitIndex),
                        config.getSchemaName(),
                        config.getTableName(),
                        splitColumn,
                        chunkStart,
                        null));
    }

    private void addSplitToPending(OceanBaseSplit split) {
        String key = splitBoundaryKey(split);
        if (!knownSplitKeys.add(key)) {
            // Split with same boundaries already exists (from checkpoint restore); skip
            return;
        }
        synchronized (pendingSplits) {
            pendingSplits.addLast(split);
        }
    }

    private static String splitBoundaryKey(OceanBaseSplit split) {
        return split.getSchemaName()
                + "."
                + split.getTableName()
                + "#"
                + split.getSplitColumn()
                + "#"
                + split.getSplitStart()
                + "#"
                + split.getSplitEnd();
    }

    private void addAllSplitsFromPoints(String splitColumn, List<Object> splitPoints) {
        synchronized (pendingSplits) {
            for (int i = 0; i < splitPoints.size() - 1; i++) {
                pendingSplits.addLast(
                        new OceanBaseSplit(
                                String.valueOf(i),
                                config.getSchemaName(),
                                config.getTableName(),
                                splitColumn,
                                splitPoints.get(i),
                                splitPoints.get(i + 1)));
            }
            dedupePendingSplits();
        }
    }

    private void signalNoMoreSplitsIfDone() {
        synchronized (pendingSplits) {
            if (pendingSplits.isEmpty() && inFlightSplits.isEmpty()) {
                for (int reader : readersAwaitingSplit) {
                    context.signalNoMoreSplits(reader);
                }
                readersAwaitingSplit.clear();
            }
        }
    }

    // ---- Split point generation ----

    private String getDefaultSplitColumn() throws SQLException {
        if (config.isOracleMode()) {
            return "ROWID";
        } else {
            return getPrimaryKeyColumn();
        }
    }

    private String getPrimaryKeyColumn() throws SQLException {
        try (Connection conn = getDataSource().getConnection()) {
            java.sql.DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs =
                    metaData.getPrimaryKeys(null, config.getSchemaName(), config.getTableName())) {
                if (rs.next()) {
                    return rs.getString("COLUMN_NAME");
                }
            }
        }

        LOG.warn(
                "No primary key found for table {}.{}, will use single split",
                config.getSchemaName(),
                config.getTableName());
        return null;
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

    private List<Object> generateNumericSplitPoints(Object min, Object max, int numSplits) {
        List<Object> points = new ArrayList<>();
        points.add(null);

        if (min instanceof BigDecimal && max instanceof BigDecimal) {
            BigDecimal minVal = (BigDecimal) min;
            BigDecimal maxVal = (BigDecimal) max;
            BigDecimal step =
                    maxVal.subtract(minVal)
                            .divide(new BigDecimal(numSplits), 16, RoundingMode.HALF_UP);

            for (int i = 1; i < numSplits; i++) {
                points.add(minVal.add(step.multiply(new BigDecimal(i))));
            }
        } else if (min instanceof Long || min instanceof Integer) {
            long minVal = ((Number) min).longValue();
            long maxVal = ((Number) max).longValue();
            long range = maxVal - minVal;
            for (int i = 1; i < numSplits; i++) {
                points.add(minVal + range * i / numSplits);
            }
        } else if (min instanceof Number && max instanceof Number) {
            double minVal = ((Number) min).doubleValue();
            double maxVal = ((Number) max).doubleValue();
            double step = (maxVal - minVal) / numSplits;

            for (int i = 1; i < numSplits; i++) {
                points.add(minVal + step * i);
            }
        }

        points.add(null);
        return dedupeAndTrimSplitPoints(points);
    }

    List<Object> generateSplitPointsForTest(
            String splitColumn, Object min, Object max, int numSplits) {
        return generateNumericSplitPoints(min, max, numSplits);
    }

    /**
     * Queries the maximum value of the next chunk starting from {@code includedLowerBound}.
     *
     * <p>For MySQL: {@code SELECT MAX(col) FROM (SELECT col FROM table WHERE col >= ? ORDER BY col
     * ASC LIMIT ?) AS T}
     *
     * <p>For Oracle: uses ROWNUM-based subquery instead of LIMIT.
     *
     * <p>Each query scans at most {@code chunkSize} rows, avoiding the deep pagination problem of
     * LIMIT/OFFSET.
     */
    private Object queryNextChunkMax(String splitColumn, int chunkSize, Object includedLowerBound) {
        String quotedSplitColumn = quoteIdentifier(splitColumn);
        String quotedTable =
                quoteIdentifier(config.getSchemaName())
                        + "."
                        + quoteIdentifier(config.getTableName());

        String sql;
        if (config.isOracleMode()) {
            sql =
                    "SELECT MAX(val) FROM (SELECT val FROM (SELECT "
                            + quotedSplitColumn
                            + " AS val FROM "
                            + quotedTable
                            + " WHERE "
                            + quotedSplitColumn
                            + " >= ? ORDER BY "
                            + quotedSplitColumn
                            + ") WHERE ROWNUM <= ?)";
        } else {
            sql =
                    "SELECT MAX("
                            + quotedSplitColumn
                            + ") FROM (SELECT "
                            + quotedSplitColumn
                            + " FROM "
                            + quotedTable
                            + " WHERE "
                            + quotedSplitColumn
                            + " >= ? ORDER BY "
                            + quotedSplitColumn
                            + " ASC LIMIT ?) AS T";
        }

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, includedLowerBound);
            stmt.setInt(2, chunkSize);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to query next chunk max from bound {}", includedLowerBound, e);
        }
        return null;
    }

    /**
     * Queries the minimum value strictly greater than {@code excludedLowerBound}. Used to skip past
     * duplicate boundaries when many rows share the same split column value.
     */
    private Object queryMinGreaterThan(String splitColumn, Object excludedLowerBound) {
        String quotedSplitColumn = quoteIdentifier(splitColumn);
        String quotedTable =
                quoteIdentifier(config.getSchemaName())
                        + "."
                        + quoteIdentifier(config.getTableName());

        String sql =
                "SELECT MIN("
                        + quotedSplitColumn
                        + ") FROM "
                        + quotedTable
                        + " WHERE "
                        + quotedSplitColumn
                        + " > ?";

        try (Connection conn = getDataSource().getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, excludedLowerBound);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to query min greater than {}", excludedLowerBound, e);
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
        return config.quoteIdentifier(identifier);
    }

    // ---- Split assignment ----

    private void assignPendingSplits() {
        List<Integer> readers = new ArrayList<>(readersAwaitingSplit);
        synchronized (pendingSplits) {
            for (int reader : readers) {
                if (!pendingSplits.isEmpty() && !inFlightSplits.containsKey(reader)) {
                    assignSplitToReader(reader);
                }
            }
        }
    }

    /** Must be called while holding the pendingSplits lock. */
    private void assignSplitToReader(int subtaskId) {
        if (inFlightSplits.containsKey(subtaskId)) {
            return;
        }

        if (pendingSplits.isEmpty()) {
            if (splitDiscoveryFinished && inFlightSplits.isEmpty()) {
                readersAwaitingSplit.remove(subtaskId);
                context.signalNoMoreSplits(subtaskId);
            }
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
                    dataSource = config.createConfiguredDataSource();
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
