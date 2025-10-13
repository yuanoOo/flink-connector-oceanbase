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
import com.oceanbase.connector.flink.connection.OBKVHBaseConnectionProvider;
import com.oceanbase.connector.flink.table.HTableInfo;
import com.oceanbase.connector.flink.table.OBKVHBaseRowDataDeserializer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Lookup function for OBKV-HBase connector. */
public class OBKVHBaseLookupFunction extends TableFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseLookupFunction.class);

    private final OBKVHBaseConnectorOptions options;
    private final HTableInfo tableInfo;
    private final int[][] lookupKeys;

    private transient OBKVHBaseConnectionProvider connectionProvider;
    private transient Table table;
    private transient OBKVHBaseRowDataDeserializer deserializer;
    private transient Cache<RowData, RowData> cache;

    public OBKVHBaseLookupFunction(
            OBKVHBaseConnectorOptions options, HTableInfo tableInfo, int[][] lookupKeys) {
        this.options = options;
        this.tableInfo = tableInfo;
        this.lookupKeys = lookupKeys;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.info("Opening OBKVHBaseLookupFunction");

        // Initialize connection provider
        connectionProvider = new OBKVHBaseConnectionProvider(options);
        table = connectionProvider.getHTableClient(tableInfo.getTableId());

        // Initialize deserializer
        deserializer = new OBKVHBaseRowDataDeserializer(tableInfo);

        // Initialize cache if enabled
        if (options.getLookupCacheEnabled()) {
            cache =
                    CacheBuilder.newBuilder()
                            .maximumSize(options.getLookupCacheMaxRows())
                            .expireAfterWrite(
                                    options.getLookupCacheTTL().toMillis(), TimeUnit.MILLISECONDS)
                            .build();
            LOG.info(
                    "Lookup cache enabled with max rows: {} and TTL: {}",
                    options.getLookupCacheMaxRows(),
                    options.getLookupCacheTTL());
        }
    }

    public void eval(Object... keys) {
        if (keys == null || keys.length == 0) {
            return;
        }

        try {
            // Create cache key if cache is enabled
            RowData cacheKey = null;
            if (cache != null) {
                cacheKey = createCacheKey(keys);
                RowData cachedValue = cache.getIfPresent(cacheKey);
                if (cachedValue != null) {
                    collect(cachedValue);
                    return;
                }
            }

            // Serialize row key
            byte[] rowKeyBytes = serializeRowKey(keys[0]);

            // Create Get request
            Get get = new Get(rowKeyBytes);

            // Add families to the Get request
            for (String familyName : tableInfo.getFamilyNames()) {
                get.addFamily(Bytes.toBytes(familyName));
            }

            // Execute query
            Result result = table.get(get);

            // Deserialize and collect result
            if (result != null && !result.isEmpty()) {
                RowData rowData = deserializer.deserialize(result);
                if (rowData != null) {
                    // Update cache if enabled
                    if (cache != null && cacheKey != null) {
                        cache.put(cacheKey, rowData);
                    }
                    collect(rowData);
                }
            }
        } catch (IOException e) {
            LOG.error("Error during lookup", e);
            throw new RuntimeException("Error during lookup", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing OBKVHBaseLookupFunction");
        if (cache != null) {
            cache.invalidateAll();
            cache.cleanUp();
        }
        if (connectionProvider != null) {
            connectionProvider.close();
        }
        super.close();
    }

    private byte[] serializeRowKey(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("Row key cannot be null");
        }

        LogicalType rowKeyType = tableInfo.getRowKeyType();
        switch (rowKeyType.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return Bytes.toBytes(key.toString());
            case BOOLEAN:
                return Bytes.toBytes((Boolean) key);
            case TINYINT:
                return new byte[] {((Number) key).byteValue()};
            case SMALLINT:
                return Bytes.toBytes(((Number) key).shortValue());
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return Bytes.toBytes(((Number) key).intValue());
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Bytes.toBytes(((Number) key).longValue());
            case FLOAT:
                return Bytes.toBytes(((Number) key).floatValue());
            case DOUBLE:
                return Bytes.toBytes(((Number) key).doubleValue());
            case BINARY:
            case VARBINARY:
                return (byte[]) key;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported rowkey type: " + rowKeyType.getTypeRoot());
        }
    }

    private RowData createCacheKey(Object... keys) {
        // Simple cache key implementation using the first lookup key
        // In production, you might want a more sophisticated cache key strategy
        return org.apache.flink.table.data.GenericRowData.of(keys);
    }
}
