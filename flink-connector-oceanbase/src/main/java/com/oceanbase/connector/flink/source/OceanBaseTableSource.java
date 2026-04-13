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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.types.DataType;

/** OceanBase table source for SQL API. */
public class OceanBaseTableSource implements ScanTableSource {

    private final OceanBaseSourceConfig config;
    private final DataType producedDataType;

    public OceanBaseTableSource(OceanBaseSourceConfig config, DataType producedDataType) {
        this.config = config;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceProvider.of(new OceanBaseSource(config, producedDataType));
    }

    @Override
    public DynamicTableSource copy() {
        return new OceanBaseTableSource(config, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase";
    }
}
