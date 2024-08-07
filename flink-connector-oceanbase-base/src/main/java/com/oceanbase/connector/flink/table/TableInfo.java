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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TableInfo implements Table {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final List<String> primaryKey;
    private final List<String> fieldNames;
    private final Map<String, Integer> fieldIndexMap;
    private final List<LogicalType> dataTypes;
    private final SerializableFunction<String, String> placeholderFunc;

    public TableInfo(TableId tableId, ResolvedSchema resolvedSchema) {
        this(
                tableId,
                resolvedSchema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(null),
                resolvedSchema.getColumnNames(),
                resolvedSchema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList()),
                null);
    }

    public TableInfo(
            @Nonnull TableId tableId,
            @Nullable List<String> primaryKey,
            @Nonnull List<String> fieldNames,
            @Nonnull List<LogicalType> dataTypes,
            @Nullable SerializableFunction<String, String> placeholderFunc) {
        this.tableId = tableId;
        this.primaryKey = primaryKey;
        this.fieldNames = fieldNames;
        this.dataTypes = dataTypes;
        this.fieldIndexMap =
                IntStream.range(0, fieldNames.size())
                        .boxed()
                        .collect(Collectors.toMap(fieldNames::get, i -> i));
        this.placeholderFunc = placeholderFunc;
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    @Override
    public List<String> getKey() {
        return primaryKey;
    }

    @Override
    public int getFieldIndex(String fieldName) {
        return checkNotNull(
                fieldIndexMap.get(fieldName), String.format("Field '%s' not found", fieldName));
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<LogicalType> getDataTypes() {
        return dataTypes;
    }

    public SerializableFunction<String, String> getPlaceholderFunc() {
        return placeholderFunc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo that = (TableInfo) o;
        return Objects.equals(this.tableId, that.tableId)
                && Objects.equals(this.primaryKey, that.primaryKey)
                && Objects.equals(this.fieldNames, that.fieldNames)
                && Objects.equals(this.fieldIndexMap, that.fieldIndexMap)
                && Objects.equals(this.dataTypes, that.dataTypes);
    }
}
