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

package com.oceanbase.connector.flink;

import com.oceanbase.connector.flink.sink.FileCompletionColumns;
import com.oceanbase.connector.flink.sink.OceanBaseDynamicTableSink;
import com.oceanbase.connector.flink.source.OceanBaseTableSourceFactory;
import com.oceanbase.connector.flink.utils.OptionUtils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OceanBaseDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "oceanbase";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(OceanBaseConnectorOptions.FILE_COMPLETION_KAFKA_PROPS_PREFIX);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        ResolvedSchema physicalSchema =
                new ResolvedSchema(
                        resolvedSchema.getColumns().stream()
                                .filter(Column::isPhysical)
                                .collect(Collectors.toList()),
                        resolvedSchema.getWatermarkSpecs(),
                        resolvedSchema.getPrimaryKey().orElse(null));
        Map<String, String> options = context.getCatalogTable().getOptions();
        OptionUtils.printOptions(IDENTIFIER, options);
        OceanBaseConnectorOptions connectorOptions = new OceanBaseConnectorOptions(options);
        connectorOptions.validateFileCompletionOptions();
        if (connectorOptions.isFileCompletionKafkaEnabled()) {
            FileCompletionColumns.assertColumnsValid(
                    physicalSchema,
                    connectorOptions.getFileCompletionFlagColumn(),
                    connectorOptions.getFileCompletionMessageColumn());
        }
        return new OceanBaseDynamicTableSink(physicalSchema, connectorOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OceanBaseConnectorOptions.URL);
        options.add(OceanBaseConnectorOptions.USERNAME);
        options.add(OceanBaseConnectorOptions.PASSWORD);
        options.add(OceanBaseConnectorOptions.SCHEMA_NAME);
        options.add(OceanBaseConnectorOptions.TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OceanBaseConnectorOptions.SYNC_WRITE);
        options.add(OceanBaseConnectorOptions.BUFFER_FLUSH_INTERVAL);
        options.add(OceanBaseConnectorOptions.BUFFER_SIZE);
        options.add(OceanBaseConnectorOptions.MAX_RETRIES);
        options.add(OceanBaseConnectorOptions.DRIVER_CLASS_NAME);
        options.add(OceanBaseConnectorOptions.DRUID_PROPERTIES);
        options.add(OceanBaseConnectorOptions.MEMSTORE_CHECK_ENABLED);
        options.add(OceanBaseConnectorOptions.MEMSTORE_THRESHOLD);
        options.add(OceanBaseConnectorOptions.MEMSTORE_CHECK_INTERVAL);
        options.add(OceanBaseConnectorOptions.PARTITION_ENABLED);
        options.add(OceanBaseConnectorOptions.TABLE_ORACLE_TENANT_CASE_INSENSITIVE);
        options.add(ConnectorOptions.SINK_PARALLELISM);
        // Tolerate source-only options since both factories share the "oceanbase" identifier
        options.add(OceanBaseTableSourceFactory.COMPATIBLE_MODE);
        options.add(OceanBaseTableSourceFactory.SPLIT_SIZE);
        options.add(OceanBaseTableSourceFactory.CHUNK_KEY_COLUMN);
        options.add(OceanBaseTableSourceFactory.FETCH_SIZE);
        options.add(OceanBaseConnectorOptions.FILE_COMPLETION_FLAG_COLUMN);
        options.add(OceanBaseConnectorOptions.FILE_COMPLETION_MESSAGE_COLUMN);
        options.add(OceanBaseConnectorOptions.FILE_COMPLETION_KAFKA_TOPIC);
        options.add(OceanBaseConnectorOptions.FILE_COMPLETION_KAFKA_NOTIFICATION_ENABLED);
        return options;
    }
}
