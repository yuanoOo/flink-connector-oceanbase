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

import com.oceanbase.connector.flink.OBKVHBaseConnectorOptions;
import com.oceanbase.connector.flink.table.HTableInfo;
import com.oceanbase.connector.flink.table.TableId;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;

/** DynamicTableSource implementation for OBKV-HBase. */
public class OBKVHBaseDynamicTableSource
        implements LookupTableSource, ScanTableSource, SupportsProjectionPushDown {

    private final ResolvedSchema physicalSchema;
    private final OBKVHBaseConnectorOptions options;
    private final HTableInfo tableInfo;

    public OBKVHBaseDynamicTableSource(
            ResolvedSchema physicalSchema, OBKVHBaseConnectorOptions options) {
        this.physicalSchema = physicalSchema;
        this.options = options;
        this.tableInfo =
                new HTableInfo(
                        new TableId(options.getSchemaName(), options.getTableName()),
                        physicalSchema);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(
                new OBKVHBaseLookupFunction(options, tableInfo, context.getKeys()));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        return InputFormatProvider.of(new OBKVHBaseRowDataInputFormat(options, tableInfo));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        return new OBKVHBaseDynamicTableSource(physicalSchema, options);
    }

    @Override
    public String asSummaryString() {
        return "OBKV-HBase-Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // Projection push down is not supported yet
    }
}
