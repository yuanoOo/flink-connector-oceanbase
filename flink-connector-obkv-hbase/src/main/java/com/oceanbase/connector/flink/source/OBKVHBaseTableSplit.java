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

import org.apache.flink.core.io.LocatableInputSplit;

/**
 * Input split for OBKV-HBase table scanning. Each split corresponds to a region in the HBase table
 * and includes hostname information for data locality optimization.
 */
public class OBKVHBaseTableSplit extends LocatableInputSplit {

    private static final long serialVersionUID = 1L;

    /** The name of the table to retrieve data from. */
    private final byte[] tableName;

    /** The start row of the split. */
    private final byte[] startRow;

    /** The end row of the split. */
    private final byte[] endRow;

    /**
     * Creates a new OBKV-HBase table input split.
     *
     * @param splitNumber the number of the input split
     * @param hostnames the names of the hosts storing the data (for data locality)
     * @param tableName the name of the table to retrieve data from
     * @param startRow the start row of the split
     * @param endRow the end row of the split
     */
    public OBKVHBaseTableSplit(
            int splitNumber, String[] hostnames, byte[] tableName, byte[] startRow, byte[] endRow) {
        super(splitNumber, hostnames);
        this.tableName = tableName;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    /**
     * Returns the table name.
     *
     * @return The table name.
     */
    public byte[] getTableName() {
        return tableName;
    }

    /**
     * Returns the start row.
     *
     * @return The start row.
     */
    public byte[] getStartRow() {
        return startRow;
    }

    /**
     * Returns the end row.
     *
     * @return The end row.
     */
    public byte[] getEndRow() {
        return endRow;
    }
}
