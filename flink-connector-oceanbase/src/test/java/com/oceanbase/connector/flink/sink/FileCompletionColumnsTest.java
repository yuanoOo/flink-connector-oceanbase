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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link FileCompletionColumns}. */
class FileCompletionColumnsTest {

    private static final String FLAG = "is_eof";
    private static final String MSG = "kafka_msg";

    // ---------------------------------------------------------------------------------------------
    // stripFromSchema
    // ---------------------------------------------------------------------------------------------

    @Test
    void stripFromSchema_removesFlagAndMessageColumns() {
        ResolvedSchema full =
                schema(
                        col("id", DataTypes.BIGINT()),
                        col("name", DataTypes.STRING()),
                        col(FLAG, DataTypes.BOOLEAN()),
                        col(MSG, DataTypes.STRING()));

        ResolvedSchema stripped = FileCompletionColumns.stripFromSchema(full, FLAG, MSG);

        assertEquals(Arrays.asList("id", "name"), stripped.getColumnNames());
        assertFalse(stripped.getPrimaryKey().isPresent());
    }

    @Test
    void stripFromSchema_preservesPrimaryKeyWhenNotFileCompletionColumn() {
        ResolvedSchema full =
                new ResolvedSchema(
                        Arrays.asList(
                                col("id", DataTypes.BIGINT().notNull()),
                                col("name", DataTypes.STRING()),
                                col(FLAG, DataTypes.BOOLEAN()),
                                col(MSG, DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

        ResolvedSchema stripped = FileCompletionColumns.stripFromSchema(full, FLAG, MSG);

        assertTrue(stripped.getPrimaryKey().isPresent());
        assertEquals(Collections.singletonList("id"), stripped.getPrimaryKey().get().getColumns());
    }

    @Test
    void stripFromSchema_throwsWhenPrimaryKeyIsEntirelyFileCompletionColumns() {
        ResolvedSchema full =
                new ResolvedSchema(
                        Arrays.asList(
                                col("id", DataTypes.BIGINT()),
                                col(FLAG, DataTypes.BOOLEAN().notNull()),
                                col(MSG, DataTypes.STRING().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Arrays.asList(FLAG, MSG)));

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> FileCompletionColumns.stripFromSchema(full, FLAG, MSG));
        assertTrue(ex.getMessage().contains("Primary key"));
    }

    @Test
    void stripFromSchema_keepsRemainingPkColumnsWhenSomePkIsFileCompletion() {
        ResolvedSchema full =
                new ResolvedSchema(
                        Arrays.asList(
                                col("id", DataTypes.BIGINT().notNull()),
                                col(FLAG, DataTypes.BOOLEAN().notNull()),
                                col(MSG, DataTypes.STRING())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Arrays.asList("id", FLAG)));

        ResolvedSchema stripped = FileCompletionColumns.stripFromSchema(full, FLAG, MSG);

        assertEquals(Collections.singletonList("id"), stripped.getPrimaryKey().get().getColumns());
    }

    // ---------------------------------------------------------------------------------------------
    // obColumnIndicesInFullRow
    // ---------------------------------------------------------------------------------------------

    @Test
    void obColumnIndicesInFullRow_returnsBusinessColumnIndices() {
        ResolvedSchema full =
                schema(
                        col("id", DataTypes.BIGINT()),
                        col(FLAG, DataTypes.BOOLEAN()),
                        col("name", DataTypes.STRING()),
                        col(MSG, DataTypes.STRING()),
                        col("amount", DataTypes.DECIMAL(10, 2)));

        int[] indices = FileCompletionColumns.obColumnIndicesInFullRow(full, FLAG, MSG);

        assertArrayEquals(new int[] {0, 2, 4}, indices);
    }

    @Test
    void obColumnIndicesInFullRow_emptyWhenOnlyMetaColumnsExist() {
        ResolvedSchema full = schema(col(FLAG, DataTypes.BOOLEAN()), col(MSG, DataTypes.STRING()));

        int[] indices = FileCompletionColumns.obColumnIndicesInFullRow(full, FLAG, MSG);

        assertArrayEquals(new int[0], indices);
    }

    // ---------------------------------------------------------------------------------------------
    // resolvePhysicalColumnIndex
    // ---------------------------------------------------------------------------------------------

    @Test
    void resolvePhysicalColumnIndex_findsExistingColumn() {
        ResolvedSchema full =
                schema(
                        col("id", DataTypes.BIGINT()),
                        col(FLAG, DataTypes.BOOLEAN()),
                        col(MSG, DataTypes.STRING()));

        assertEquals(1, FileCompletionColumns.resolvePhysicalColumnIndex(full, FLAG));
        assertEquals(2, FileCompletionColumns.resolvePhysicalColumnIndex(full, MSG));
    }

    @Test
    void resolvePhysicalColumnIndex_isCaseSensitive() {
        ResolvedSchema full = schema(col(FLAG, DataTypes.BOOLEAN()));

        assertThrows(
                IllegalArgumentException.class,
                () -> FileCompletionColumns.resolvePhysicalColumnIndex(full, FLAG.toUpperCase()));
    }

    @Test
    void resolvePhysicalColumnIndex_throwsWhenMissing() {
        ResolvedSchema full = schema(col("id", DataTypes.BIGINT()));

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> FileCompletionColumns.resolvePhysicalColumnIndex(full, "missing"));
        assertTrue(ex.getMessage().contains("missing"));
        assertTrue(ex.getMessage().contains("Available"));
    }

    // ---------------------------------------------------------------------------------------------
    // assertColumnsValid
    // ---------------------------------------------------------------------------------------------

    @Test
    void assertColumnsValid_passesForBooleanFlagAndStringMessage() {
        ResolvedSchema full =
                schema(
                        col("id", DataTypes.BIGINT()),
                        col(FLAG, DataTypes.BOOLEAN()),
                        col(MSG, DataTypes.VARCHAR(128)));

        FileCompletionColumns.assertColumnsValid(full, FLAG, MSG);
    }

    @Test
    void assertColumnsValid_passesForCharMessage() {
        ResolvedSchema full = schema(col(FLAG, DataTypes.BOOLEAN()), col(MSG, DataTypes.CHAR(32)));

        FileCompletionColumns.assertColumnsValid(full, FLAG, MSG);
    }

    @Test
    void assertColumnsValid_rejectsNonBooleanFlag() {
        ResolvedSchema full = schema(col(FLAG, DataTypes.INT()), col(MSG, DataTypes.STRING()));

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> FileCompletionColumns.assertColumnsValid(full, FLAG, MSG));
        assertTrue(ex.getMessage().contains(FLAG));
        assertTrue(ex.getMessage().contains("BOOLEAN"));
    }

    @Test
    void assertColumnsValid_rejectsNonStringMessage() {
        ResolvedSchema full = schema(col(FLAG, DataTypes.BOOLEAN()), col(MSG, DataTypes.INT()));

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> FileCompletionColumns.assertColumnsValid(full, FLAG, MSG));
        assertTrue(ex.getMessage().contains(MSG));
        assertTrue(ex.getMessage().contains("CHAR or VARCHAR"));
    }

    @Test
    void assertColumnsValid_rejectsMissingColumn() {
        ResolvedSchema full = schema(col("id", DataTypes.BIGINT()));

        assertThrows(
                IllegalArgumentException.class,
                () -> FileCompletionColumns.assertColumnsValid(full, FLAG, MSG));
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static Column col(String name, DataType type) {
        return Column.physical(name, type);
    }

    private static ResolvedSchema schema(Column... columns) {
        return new ResolvedSchema(Arrays.asList(columns), Collections.emptyList(), null);
    }

    @SuppressWarnings("unused")
    private static List<String> names(ResolvedSchema schema) {
        return schema.getColumnNames();
    }
}
