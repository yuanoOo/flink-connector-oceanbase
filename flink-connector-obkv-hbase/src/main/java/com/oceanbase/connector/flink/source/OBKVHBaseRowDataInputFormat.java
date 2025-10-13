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

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;

import com.alipay.oceanbase.hbase.OHTableClient;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** InputFormat for scanning OBKV-HBase tables. */
public class OBKVHBaseRowDataInputFormat extends RichInputFormat<RowData, OBKVHBaseTableSplit> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OBKVHBaseRowDataInputFormat.class);

    private final OBKVHBaseConnectorOptions options;
    private final HTableInfo tableInfo;

    private transient OBKVHBaseConnectionProvider connectionProvider;
    private transient Table table;
    private transient OBKVHBaseRowDataDeserializer deserializer;
    private transient ResultScanner scanner;
    private transient Iterator<Result> resultIterator;
    private transient Result currentResult;

    public OBKVHBaseRowDataInputFormat(OBKVHBaseConnectorOptions options, HTableInfo tableInfo) {
        this.options = options;
        this.tableInfo = tableInfo;
    }

    @Override
    public void configure(Configuration parameters) {
        // Configuration is handled in constructor
    }

    @Override
    public void openInputFormat() throws IOException {
        LOG.info("Opening input format for table: {}", tableInfo.getTableId());
        connectionProvider = new OBKVHBaseConnectionProvider(options);
        table = connectionProvider.getHTableClient(tableInfo.getTableId());
        deserializer = new OBKVHBaseRowDataDeserializer(tableInfo);
    }

    @Override
    public void closeInputFormat() throws IOException {
        LOG.info("Closing input format");
        if (connectionProvider != null) {
            try {
                connectionProvider.close();
            } catch (Exception e) {
                LOG.error("Error closing connection provider", e);
                throw new IOException("Error closing connection provider", e);
            }
        }
    }

    @Override
    public void open(OBKVHBaseTableSplit split) throws IOException {
        LOG.info(
                "Opening split: start={}, end={}",
                Bytes.toStringBinary(split.getStartRow()),
                Bytes.toStringBinary(split.getEndRow()));

        // Create scan
        Scan scan = new Scan();
        scan.withStartRow(split.getStartRow());
        scan.withStopRow(split.getEndRow());
        scan.setCaching(options.getScanCaching());

        // Add families
        for (String familyName : tableInfo.getFamilyNames()) {
            scan.addFamily(Bytes.toBytes(familyName));
        }

        // Execute scan
        scanner = table.getScanner(scan);
        resultIterator = scanner.iterator();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing scanner");
        if (scanner != null) {
            scanner.close();
        }
        currentResult = null;
        resultIterator = null;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !resultIterator.hasNext();
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        currentResult = resultIterator.next();
        return deserializer.deserialize(currentResult);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public OBKVHBaseTableSplit[] createInputSplits(int minNumSplits) throws IOException {
        LOG.info(
                "Creating input splits for table: {}, minNumSplits: {}",
                tableInfo.getTableId(),
                minNumSplits);

        // Create temporary connection to get region information
        OBKVHBaseConnectionProvider tempConnectionProvider = null;
        Table tempTable = null;

        try {
            // 1. Initialize temporary connection (similar to Flink HBase Connector's initTable())
            tempConnectionProvider = new OBKVHBaseConnectionProvider(options);
            tempTable = tempConnectionProvider.getHTableClient(tableInfo.getTableId());

            // 2. Get start/end keys using OBKV-HBase API
            Pair<byte[][], byte[][]> startEndKeys;
            if (tempTable instanceof OHTableClient) {
                startEndKeys = ((OHTableClient) tempTable).getStartEndKeys();
            } else {
                throw new IOException("Unsupported table type: " + tempTable.getClass());
            }

            byte[][] startKeys = startEndKeys.getFirst();
            byte[][] endKeys = startEndKeys.getSecond();

            if (startKeys == null || startKeys.length == 0) {
                LOG.warn(
                        "No regions found for table {}, returning empty splits",
                        tableInfo.getTableId());
                return new OBKVHBaseTableSplit[0];
            }

            // 3. Create RegionLocator to get hostname information (for data locality)
            RegionLocator regionLocator = null;
            try {
                // Try to get RegionLocator via ConnectionFactory
                org.apache.hadoop.conf.Configuration hbaseConfig = tempTable.getConfiguration();
                Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                TableName tableName = TableName.valueOf(tableInfo.getTableId().getTableName());
                regionLocator = connection.getRegionLocator(tableName);
            } catch (Exception e) {
                LOG.debug("Failed to create RegionLocator, will use empty hosts", e);
            }

            // 4. Create a split for each region (following Flink HBase Connector pattern)
            List<OBKVHBaseTableSplit> splits = new ArrayList<>();
            byte[] tableName = tableInfo.getTableId().getTableName().getBytes();

            for (int i = 0; i < startKeys.length; i++) {
                byte[] startKey = startKeys[i];
                byte[] endKey = endKeys[i];

                // Try to get region location (hostname) for data locality
                String[] hosts = new String[0];
                if (regionLocator != null) {
                    try {
                        HRegionLocation location = regionLocator.getRegionLocation(startKey, false);
                        if (location != null) {
                            String hostnamePort = location.getHostnamePort();
                            hosts = new String[] {hostnamePort};
                            LOG.debug("Region {} located at: {}", i, hostnamePort);
                        }
                    } catch (Exception e) {
                        LOG.debug(
                                "Failed to get region location for split {}, using empty hosts",
                                i,
                                e);
                    }
                }

                // Create split with hostname information
                int splitId = splits.size();
                OBKVHBaseTableSplit split =
                        new OBKVHBaseTableSplit(splitId, hosts, tableName, startKey, endKey);
                splits.add(split);

                if (LOG.isDebugEnabled()) {
                    logSplitInfo("created", split);
                }
            }

            LOG.info(
                    "Created {} input splits based on {} regions", splits.size(), startKeys.length);

            return splits.toArray(new OBKVHBaseTableSplit[0]);

        } finally {
            // 5. Close temporary connection (following Flink HBase Connector's closeTable())
            if (tempConnectionProvider != null) {
                try {
                    tempConnectionProvider.close();
                } catch (Exception e) {
                    LOG.warn("Error closing temporary connection provider", e);
                }
            }
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(OBKVHBaseTableSplit[] inputSplits) {
        // Use LocatableInputSplitAssigner for data locality optimization
        return new LocatableInputSplitAssigner(inputSplits);
    }

    /** Log split information (following Flink HBase Connector pattern). */
    private void logSplitInfo(String action, OBKVHBaseTableSplit split) {
        int splitId = split.getSplitNumber();
        String splitStart = Bytes.toString(split.getStartRow());
        String splitEnd = Bytes.toString(split.getEndRow());
        String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
        String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
        String[] hostnames = split.getHostnames();
        LOG.info(
                "{} split (this={})[id={}|hosts={}|start={}|end={}]",
                action,
                this,
                splitId,
                hostnames.length > 0 ? hostnames[0] : "none",
                splitStartKey,
                splitStopKey);
    }
}
