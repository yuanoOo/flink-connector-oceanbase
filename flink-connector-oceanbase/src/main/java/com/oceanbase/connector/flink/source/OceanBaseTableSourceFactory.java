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

import com.oceanbase.connector.flink.ConnectorOptions;
import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/** Factory for OceanBase table source. */
public class OceanBaseTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "oceanbase";

    public static final ConfigOption<String> COMPATIBLE_MODE =
            ConfigOptions.key("compatible-mode")
                    .stringType()
                    .defaultValue("MySQL")
                    .withDescription(
                            "The compatible mode of OceanBase, can be 'MySQL' or 'Oracle'.");

    public static final ConfigOption<Integer> SPLIT_SIZE =
            ConfigOptions.key("split-size")
                    .intType()
                    .defaultValue(8192)
                    .withDescription("The number of rows per split.");

    public static final ConfigOption<String> CHUNK_KEY_COLUMN =
            ConfigOptions.key("chunk-key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The chunk key column for splitting.");

    public static final ConfigOption<Integer> FETCH_SIZE =
            ConfigOptions.key("fetch-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The fetch size for JDBC query.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ConnectorOptions.URL);
        options.add(ConnectorOptions.USERNAME);
        options.add(ConnectorOptions.PASSWORD);
        options.add(ConnectorOptions.SCHEMA_NAME);
        options.add(ConnectorOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        // Source-specific options
        options.add(COMPATIBLE_MODE);
        options.add(SPLIT_SIZE);
        options.add(CHUNK_KEY_COLUMN);
        options.add(FETCH_SIZE);
        // Tolerate sink-only options since both factories share the "oceanbase" identifier
        options.add(ConnectorOptions.SYNC_WRITE);
        options.add(ConnectorOptions.BUFFER_FLUSH_INTERVAL);
        options.add(ConnectorOptions.BUFFER_SIZE);
        options.add(ConnectorOptions.MAX_RETRIES);
        options.add(OceanBaseConnectorOptions.DRIVER_CLASS_NAME);
        options.add(OceanBaseConnectorOptions.DRUID_PROPERTIES);
        options.add(OceanBaseConnectorOptions.MEMSTORE_CHECK_ENABLED);
        options.add(OceanBaseConnectorOptions.MEMSTORE_THRESHOLD);
        options.add(OceanBaseConnectorOptions.MEMSTORE_CHECK_INTERVAL);
        options.add(OceanBaseConnectorOptions.PARTITION_ENABLED);
        options.add(OceanBaseConnectorOptions.TABLE_ORACLE_TENANT_CASE_INSENSITIVE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();

        OceanBaseSourceConfig sourceConfig =
                new OceanBaseSourceConfig(
                        config.get(ConnectorOptions.URL),
                        config.get(ConnectorOptions.USERNAME),
                        config.get(ConnectorOptions.PASSWORD),
                        config.get(ConnectorOptions.SCHEMA_NAME),
                        config.get(ConnectorOptions.TABLE_NAME),
                        config.get(COMPATIBLE_MODE),
                        config.get(SPLIT_SIZE),
                        config.get(CHUNK_KEY_COLUMN),
                        config.get(FETCH_SIZE),
                        config.get(OceanBaseConnectorOptions.TABLE_ORACLE_TENANT_CASE_INSENSITIVE),
                        config.get(OceanBaseConnectorOptions.DRIVER_CLASS_NAME),
                        config.getOptional(OceanBaseConnectorOptions.DRUID_PROPERTIES)
                                .orElse(null));

        DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new OceanBaseTableSource(sourceConfig, producedDataType);
    }
}
