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

import java.io.Serializable;

/** Configuration for OceanBase source. */
public class OceanBaseSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String schemaName;
    private final String tableName;
    private final String compatibleMode;
    private final int splitSize;
    private final String chunkKeyColumn;
    private final int fetchSize;

    public OceanBaseSourceConfig(
            String url,
            String username,
            String password,
            String schemaName,
            String tableName,
            String compatibleMode,
            int splitSize,
            String chunkKeyColumn,
            int fetchSize) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.compatibleMode = compatibleMode;
        this.splitSize = splitSize;
        this.chunkKeyColumn = chunkKeyColumn;
        this.fetchSize = fetchSize;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public boolean isMySqlMode() {
        return "MySQL".equalsIgnoreCase(compatibleMode);
    }

    public boolean isOracleMode() {
        return "Oracle".equalsIgnoreCase(compatibleMode);
    }

    public int getSplitSize() {
        return splitSize;
    }

    public String getChunkKeyColumn() {
        return chunkKeyColumn;
    }

    public int getFetchSize() {
        return fetchSize;
    }
}
