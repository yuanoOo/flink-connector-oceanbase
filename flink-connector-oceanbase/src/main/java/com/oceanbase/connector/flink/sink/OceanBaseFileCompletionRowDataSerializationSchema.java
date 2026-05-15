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

import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.FileCompletionDataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * Serializes full {@link RowData} (incl. OSS meta columns) to OB {@link DataChangeRecord}, wrapping
 * the result as {@link FileCompletionDataChangeRecord} when the flag column is true.
 *
 * <p>Flag column must be {@code BOOLEAN} and message column must be {@code CHAR/VARCHAR}; the
 * factory validates this before runtime.
 */
public class OceanBaseFileCompletionRowDataSerializationSchema
        implements RecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final OceanBaseRowDataSerializationSchema delegate;
    private final int flagPos;
    private final int messagePos;
    private final RowData.FieldGetter[] obFieldGetters;

    public OceanBaseFileCompletionRowDataSerializationSchema(
            TableInfo obTableInfo,
            ResolvedSchema physicalFull,
            String flagColumn,
            String messageColumn) {
        this.delegate = new OceanBaseRowDataSerializationSchema(obTableInfo);
        this.flagPos = FileCompletionColumns.resolvePhysicalColumnIndex(physicalFull, flagColumn);
        this.messagePos =
                FileCompletionColumns.resolvePhysicalColumnIndex(physicalFull, messageColumn);
        int[] obIndices =
                FileCompletionColumns.obColumnIndicesInFullRow(
                        physicalFull, flagColumn, messageColumn);
        this.obFieldGetters = new RowData.FieldGetter[obIndices.length];
        for (int i = 0; i < obIndices.length; i++) {
            obFieldGetters[i] =
                    RowData.createFieldGetter(obTableInfo.getDataTypes().get(i), obIndices[i]);
        }
    }

    @Override
    public Record serialize(RowData rowData) {
        Record inner = delegate.serialize(projectObRow(rowData));
        if (rowData.isNullAt(flagPos) || !rowData.getBoolean(flagPos)) {
            return inner;
        }
        String message =
                rowData.isNullAt(messagePos) ? "" : rowData.getString(messagePos).toString();
        return new FileCompletionDataChangeRecord((DataChangeRecord) inner, message);
    }

    private GenericRowData projectObRow(RowData fullRow) {
        int arity = obFieldGetters.length;
        GenericRowData out = new GenericRowData(arity);
        out.setRowKind(fullRow.getRowKind());
        for (int i = 0; i < arity; i++) {
            out.setField(i, obFieldGetters[i].getFieldOrNull(fullRow));
        }
        return out;
    }
}
