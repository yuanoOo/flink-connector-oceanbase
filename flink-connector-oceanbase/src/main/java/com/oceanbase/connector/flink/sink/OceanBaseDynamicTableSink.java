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

package com.oceanbase.connector.flink.sink;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.sink.directload.DirectLoadSink;
import com.oceanbase.connector.flink.sink.directload.ExecutionIdBroadcastProcessFunction;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.OceanBaseRowDataSerializationSchema;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.connector.flink.table.TransactionRecord;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class OceanBaseDynamicTableSink extends AbstractDynamicTableSink {

    private final OceanBaseConnectorOptions connectorOptions;
    private final RuntimeExecutionMode runtimeExecutionMode;
    public static final MapStateDescriptor<String, String> DIRECT_LOAD_SHARE_EXECUTION_ID =
            new MapStateDescriptor<>("EXECUTION_Id", Types.STRING, Types.STRING);

    public OceanBaseDynamicTableSink(
            ResolvedSchema physicalSchema,
            OceanBaseConnectorOptions connectorOptions,
            RuntimeExecutionMode runtimeExecutionMode) {
        super(physicalSchema);
        this.connectorOptions = connectorOptions;
        this.runtimeExecutionMode = runtimeExecutionMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        TableId tableId =
                new TableId(
                        connectionProvider.getDialect()::getFullTableName,
                        connectorOptions.getSchemaName(),
                        connectorOptions.getTableName());
        OceanBaseRecordFlusher recordFlusher =
                new OceanBaseRecordFlusher(connectorOptions, connectionProvider);

        if (context.isBounded()
                && runtimeExecutionMode == RuntimeExecutionMode.BATCH
                && connectorOptions.getDirectLoadEnabled()) {
            return new DirectLoadStreamSinkProvider(
                    (dataStream) -> {
                        DataStream<RowData> input =
                                new DataStream<>(
                                        dataStream.getExecutionEnvironment(),
                                        dataStream.getTransformation());

                        StreamExecutionEnvironment env = input.getExecutionEnvironment();
                        List<Tuple3<String, String, String>> sessions =
                                DirectLoadUtils.initExecutionIdFromConnOptions(connectorOptions);
                        BroadcastStream<Tuple3<String, String, String>> broadcast =
                                env.fromCollection(sessions)
                                        .setParallelism(1)
                                        .broadcast(DIRECT_LOAD_SHARE_EXECUTION_ID);

                        ExecutionIdBroadcastProcessFunction<RowData> write =
                                new ExecutionIdBroadcastProcessFunction<>(
                                        connectorOptions,
                                        new OceanBaseRowDataSerializationSchema(
                                                new TableInfo(tableId, physicalSchema)),
                                        DataChangeRecord.KeyExtractor.simple());
                        // write with share session id
                        SingleOutputStreamOperator<String> writeOperator =
                                dataStream
                                        .connect(broadcast)
                                        .process(write)
                                        .setParallelism(env.getParallelism());
                        // commiter sink
                        return writeOperator
                                .sinkTo(new DirectLoadSink(connectorOptions))
                                .setParallelism(1);
                    });
        } else {
            return new SinkProvider(
                    typeSerializer ->
                            new OceanBaseSink<>(
                                    connectorOptions,
                                    typeSerializer,
                                    new OceanBaseRowDataSerializationSchema(
                                            new TableInfo(tableId, physicalSchema)),
                                    DataChangeRecord.KeyExtractor.simple(),
                                    recordFlusher,
                                    getWriterEventListener(recordFlusher, tableId)));
        }
    }

    protected OceanBaseWriterEvent.Listener getWriterEventListener(
            RecordFlusher recordFlusher, TableId tableId) {
        return (event) -> {
            try {
                if (event == OceanBaseWriterEvent.INITIALIZED) {
                    recordFlusher.flush(
                            new TransactionRecord(tableId, TransactionRecord.Type.BEGIN));
                }
                if (event == OceanBaseWriterEvent.CLOSING) {
                    recordFlusher.flush(
                            new TransactionRecord(tableId, TransactionRecord.Type.COMMIT));
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to flush transaction record", e);
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new OceanBaseDynamicTableSink(
                physicalSchema, connectorOptions, runtimeExecutionMode);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase";
    }
}
