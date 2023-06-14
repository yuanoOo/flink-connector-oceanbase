/*
 * Copyright (c) 2023 OceanBase
 * flink-connector-oceanbase is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.oceanbase.connector.flink.dialect;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.Collectors;

public class OceanBaseMySQLDialect implements OceanBaseDialect {

    @Override
    public String quoteIdentifier(@Nonnull String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String getUpsertStatement(
            @Nonnull String tableName,
            @Nonnull List<String> fieldNames,
            @Nonnull List<String> uniqueKeyFields) {
        String updateClause =
                fieldNames.stream()
                        .filter(f -> !uniqueKeyFields.contains(f))
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return getInsertIntoStatement(tableName, fieldNames)
                + " ON DUPLICATE KEY UPDATE "
                + updateClause;
    }

    @Override
    public String getSysDatabase() {
        return "oceanbase";
    }

    @Override
    public String getSelectOBVersionStatement() {
        return "SELECT OB_VERSION()";
    }
}