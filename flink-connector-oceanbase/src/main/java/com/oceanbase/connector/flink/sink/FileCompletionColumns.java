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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Schema helpers specific to the OSS file-completion meta columns (flag + message). Only utilities
 * that deal with those two columns belong here; do not turn this into a generic schema toolbox.
 */
public final class FileCompletionColumns {

    private FileCompletionColumns() {}

    /** Strips flag/message columns from a physical schema; PK must not be solely those columns. */
    public static ResolvedSchema stripFromSchema(
            ResolvedSchema physicalFull, String flagColumn, String messageColumn) {
        List<Column> kept = new ArrayList<>();
        for (Column column : physicalFull.getColumns()) {
            if (column.isPhysical() && !isMetaColumn(column.getName(), flagColumn, messageColumn)) {
                kept.add(column);
            }
        }
        UniqueConstraint pk =
                physicalFull
                        .getPrimaryKey()
                        .map(
                                uc -> {
                                    List<String> pkCols =
                                            uc.getColumns().stream()
                                                    .filter(
                                                            name ->
                                                                    !isMetaColumn(
                                                                            name,
                                                                            flagColumn,
                                                                            messageColumn))
                                                    .collect(Collectors.toList());
                                    if (pkCols.isEmpty()) {
                                        throw new IllegalArgumentException(
                                                "Primary key cannot consist only of file-completion columns ("
                                                        + flagColumn
                                                        + ", "
                                                        + messageColumn
                                                        + ").");
                                    }
                                    return UniqueConstraint.primaryKey(uc.getName(), pkCols);
                                })
                        .orElse(null);
        return new ResolvedSchema(kept, physicalFull.getWatermarkSpecs(), pk);
    }

    /** Physical column indices in {@code physicalFull} retained in the OB write schema. */
    public static int[] obColumnIndicesInFullRow(
            ResolvedSchema physicalFull, String flagColumn, String messageColumn) {
        List<Column> columns = physicalFull.getColumns();
        List<Integer> indices = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (column.isPhysical() && !isMetaColumn(column.getName(), flagColumn, messageColumn)) {
                indices.add(i);
            }
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Resolves a column index by exact (case-sensitive) name match among physical columns. */
    public static int resolvePhysicalColumnIndex(
            ResolvedSchema physicalFull, String targetColumnName) {
        List<Column> columns = physicalFull.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (column.isPhysical() && column.getName().equals(targetColumnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Physical column '"
                        + targetColumnName
                        + "' not found in sink schema. Available: "
                        + physicalFull.getColumnNames());
    }

    /**
     * Ensures flag/message columns exist and are typed as {@code BOOLEAN} / {@code CHAR|VARCHAR}.
     */
    public static void assertColumnsValid(
            ResolvedSchema physicalFull, String flagColumn, String messageColumn) {
        assertExactType(physicalFull, flagColumn, LogicalTypeRoot.BOOLEAN);
        assertStringType(physicalFull, messageColumn);
    }

    private static void assertExactType(
            ResolvedSchema physicalFull, String columnName, LogicalTypeRoot expected) {
        int idx = resolvePhysicalColumnIndex(physicalFull, columnName);
        LogicalTypeRoot actual =
                physicalFull.getColumnDataTypes().get(idx).getLogicalType().getTypeRoot();
        if (actual != expected) {
            throw new IllegalArgumentException(
                    "Column '"
                            + columnName
                            + "' must be "
                            + expected
                            + ", but was "
                            + actual
                            + ".");
        }
    }

    private static void assertStringType(ResolvedSchema physicalFull, String columnName) {
        int idx = resolvePhysicalColumnIndex(physicalFull, columnName);
        LogicalTypeRoot actual =
                physicalFull.getColumnDataTypes().get(idx).getLogicalType().getTypeRoot();
        if (actual != LogicalTypeRoot.VARCHAR && actual != LogicalTypeRoot.CHAR) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' must be CHAR or VARCHAR, but was " + actual + ".");
        }
    }

    private static boolean isMetaColumn(
            String columnName, String flagColumn, String messageColumn) {
        return columnName.equals(flagColumn) || columnName.equals(messageColumn);
    }
}
