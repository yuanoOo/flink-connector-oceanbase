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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/** Split definition for OceanBase parallel reading. */
public class OceanBaseSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String schemaName;
    private final String tableName;
    private final String splitColumn;
    private final Object splitStart;
    private final Object splitEnd;
    private Object lastReadValue;

    public OceanBaseSplit(
            String splitId,
            String schemaName,
            String tableName,
            String splitColumn,
            Object splitStart,
            Object splitEnd) {
        this.splitId = splitId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.splitColumn = splitColumn;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSplitColumn() {
        return splitColumn;
    }

    public Object getSplitStart() {
        return splitStart;
    }

    public Object getSplitEnd() {
        return splitEnd;
    }

    public boolean isFirstSplit() {
        return splitStart == null;
    }

    public boolean isLastSplit() {
        return splitEnd == null;
    }

    public Object getLastReadValue() {
        return lastReadValue;
    }

    public void setLastReadValue(Object lastReadValue) {
        this.lastReadValue = lastReadValue;
    }

    @Override
    public String toString() {
        return "OceanBaseSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", schemaName='"
                + schemaName
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", splitColumn='"
                + splitColumn
                + '\''
                + ", splitStart="
                + splitStart
                + ", splitEnd="
                + splitEnd
                + ", lastReadValue="
                + lastReadValue
                + '}';
    }
}
