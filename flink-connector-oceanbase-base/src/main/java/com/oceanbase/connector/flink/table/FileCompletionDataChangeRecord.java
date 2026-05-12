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

package com.oceanbase.connector.flink.table;

import java.util.Objects;

/**
 * Wraps a {@link DataChangeRecord} that must be written to OceanBase, plus a Kafka notification
 * payload to send after all buffered rows and this row have been flushed successfully.
 */
public class FileCompletionDataChangeRecord implements Record {

    private static final long serialVersionUID = 1L;

    private final DataChangeRecord dataChangeRecord;
    private final String kafkaMessage;

    public FileCompletionDataChangeRecord(DataChangeRecord dataChangeRecord, String kafkaMessage) {
        this.dataChangeRecord = Objects.requireNonNull(dataChangeRecord);
        this.kafkaMessage = kafkaMessage;
    }

    @Override
    public TableId getTableId() {
        return dataChangeRecord.getTableId();
    }

    public DataChangeRecord getDataChangeRecord() {
        return dataChangeRecord;
    }

    /** May be null or empty; notifier decides how to send. */
    public String getKafkaMessage() {
        return kafkaMessage;
    }

    @Override
    public String toString() {
        return "FileCompletionDataChangeRecord{"
                + "dataChangeRecord="
                + dataChangeRecord
                + ", kafkaMessage='"
                + kafkaMessage
                + '\''
                + '}';
    }
}
