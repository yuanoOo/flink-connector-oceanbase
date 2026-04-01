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

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC URL of OceanBase server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username to connect to OceanBase.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password to connect to OceanBase.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The schema name (database name) of the table.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table name.");

    public static final ConfigOption<String> COMPATIBLE_MODE =
            ConfigOptions.key("compatible-mode")
                    .stringType()
                    .defaultValue("MySQL")
                    .withDescription(
                            "The compatible mode of OceanBase, can be 'MySQL' or 'Oracle'.");

    public static final ConfigOption<Integer> SPLIT_SIZE =
            ConfigOptions.key("split-size")
                    .intType()
                    .defaultValue(8096)
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
        options.add(URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(COMPATIBLE_MODE);
        options.add(SPLIT_SIZE);
        options.add(CHUNK_KEY_COLUMN);
        options.add(FETCH_SIZE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig config = helper.getOptions();

        OceanBaseSourceConfig sourceConfig =
                new OceanBaseSourceConfig(
                        config.get(URL),
                        config.get(USERNAME),
                        config.get(PASSWORD),
                        config.get(SCHEMA_NAME),
                        config.get(TABLE_NAME),
                        config.get(COMPATIBLE_MODE),
                        config.get(SPLIT_SIZE),
                        config.get(CHUNK_KEY_COLUMN),
                        config.get(FETCH_SIZE));

        DataType producedDataType = context.getCatalogTable().getSchema().toRowDataType();

        return new OceanBaseTableSource(sourceConfig, producedDataType);
    }
}
