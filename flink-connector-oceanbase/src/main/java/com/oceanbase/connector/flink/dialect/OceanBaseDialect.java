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

package com.oceanbase.connector.flink.dialect;

import org.apache.flink.util.function.SerializableFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public interface OceanBaseDialect extends Serializable {

    /**
     * Quotes the identifier
     *
     * @param identifier identifier
     * @return the quoted identifier
     */
    String quoteIdentifier(@Nonnull String identifier);

    /**
     * Get the full table name
     *
     * @param schemaName schema name
     * @param tableName table name
     * @return full table name
     */
    default String getFullTableName(@Nonnull String schemaName, @Nonnull String tableName) {
        return String.format("%s.%s", quoteIdentifier(schemaName), quoteIdentifier(tableName));
    }

    /**
     * Gets the placeholder for prepared statement
     *
     * @param fieldName field name
     * @param placeholderFunc user defined placeholder function
     * @return the placeholder for prepared statement
     */
    default String getPlaceholder(
            @Nonnull String fieldName,
            @Nullable SerializableFunction<String, String> placeholderFunc) {
        return placeholderFunc != null ? placeholderFunc.apply(fieldName) : "?";
    }

    /**
     * Gets the upsert statement
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param fieldNames field names list
     * @param uniqueKeyFields unique key field names list
     * @param placeholderFunc function used to get placeholder for the fields
     * @return the statement string
     */
    String getUpsertStatement(
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields,
            @Nullable SerializableFunction<String, String> placeholderFunc);

    /**
     * Gets the insert statement
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param fieldNames field names list
     * @param placeholderFunc function used to get placeholder for the fields
     * @return the statement string
     */
    default String getInsertIntoStatement(
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nullable SerializableFunction<String, String> placeholderFunc) {
        String columns =
                fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders =
                fieldNames.stream()
                        .map(f -> getPlaceholder(f, placeholderFunc))
                        .collect(Collectors.joining(", "));
        return "INSERT INTO "
                + getFullTableName(schemaName, tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    /**
     * Gets the delete statement
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param uniqueKeyFields unique key field names list
     * @return the statement string
     */
    default String getDeleteStatement(
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<String> uniqueKeyFields) {
        String conditionClause =
                uniqueKeyFields.stream()
                        .map(f -> String.format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM "
                + getFullTableName(schemaName, tableName)
                + " WHERE "
                + conditionClause;
    }

    /**
     * Get the system database name
     *
     * @return the system database name
     */
    String getSysDatabase();

    default String getMemStoreExistStatement(double threshold) {
        return "SELECT 1 FROM "
                + getSysDatabase()
                + ".GV$OB_MEMSTORE WHERE MEMSTORE_USED > MEMSTORE_LIMIT * "
                + threshold;
    }

    default String getLegacyMemStoreExistStatement(double threshold) {
        return "SELECT 1 FROM "
                + getSysDatabase()
                + ".GV$MEMSTORE WHERE TOTAL > MEM_LIMIT * "
                + threshold;
    }

    String getQueryTenantNameStatement();

    String getListSchemaStatement();

    String getListTableStatement(String schemaName);
}
