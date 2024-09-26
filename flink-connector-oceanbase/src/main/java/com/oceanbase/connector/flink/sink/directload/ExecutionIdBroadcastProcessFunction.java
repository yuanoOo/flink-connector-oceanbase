package com.oceanbase.connector.flink.sink.directload;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;
import com.oceanbase.connector.flink.sink.OceanBaseDynamicTableSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

public class ExecutionIdBroadcastProcessFunction<T>
        extends BroadcastProcessFunction<T, Tuple3<String, String, String>, String> {
    private static final Logger LOG =
            LoggerFactory.getLogger(ExecutionIdBroadcastProcessFunction.class);

    private static final long serialVersionUID = 1L;

    private final OceanBaseConnectorOptions connectorOptions;
    private final RecordSerializationSchema<T> recordSerializer;
    private final DataChangeRecord.KeyExtractor keyExtractor;
    private volatile DirectLoader directLoad = null;
    private final List<Record> buffer = Collections.synchronizedList(new ArrayList<>(1024));

    public ExecutionIdBroadcastProcessFunction(
            OceanBaseConnectorOptions options,
            RecordSerializationSchema<T> recordSerializer,
            DataChangeRecord.KeyExtractor keyExtractor) {
        this.connectorOptions = options;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(
            T value,
            BroadcastProcessFunction<T, Tuple3<String, String, String>, String>.ReadOnlyContext ctx,
            Collector<String> out)
            throws Exception {

        String executionId =
                ctx.getBroadcastState(OceanBaseDynamicTableSink.DIRECT_LOAD_SHARE_EXECUTION_ID)
                        .get(
                                String.format(
                                        "%s.%s",
                                        connectorOptions.getSchemaName(),
                                        connectorOptions.getTableName()));
        Preconditions.checkState(
                !StringUtils.isNullOrWhitespaceOnly(executionId),
                "DirectLoad execution id should not be Null.");
        if (Objects.isNull(directLoad)) {
            directLoad =
                    DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions, executionId);
            directLoad.begin();
        }

        Record record = recordSerializer.serialize(value);
        if (record == null) {
            return;
        }

        if (connectorOptions.getBufferSize() < 0) {
            ObDirectLoadBucket bucket = new ObDirectLoadBucket();
            if (record instanceof DataChangeRecord) {
                DataChangeRecord dataChangeRecord = (DataChangeRecord) record;
                TableInfo table = (TableInfo) dataChangeRecord.getTable();
                List<String> fieldNames = table.getFieldNames();
                ObObj[] array = new ObObj[fieldNames.size()];
                int index = 0;
                for (String fieldName : fieldNames) {
                    array[index++] = createObObj(dataChangeRecord.getFieldValue(fieldName));
                }
                bucket.addRow(array);
            }

            try {
                directLoad.write(bucket);
            } catch (Exception e) {
                throw new SQLException("DirectLoad write failed", e);
            }
        } else {
            buffer.add(record);
            if (buffer.size() >= 1024) {
                flush(buffer);
            }
        }
        out.collect(executionId);
    }

    @Override
    public void processBroadcastElement(
            Tuple3<String, String, String> value,
            BroadcastProcessFunction<T, Tuple3<String, String, String>, String>.Context ctx,
            Collector<String> out)
            throws Exception {
        LOG.info("Receive broadcast execution id: {}", value);
        ctx.getBroadcastState(OceanBaseDynamicTableSink.DIRECT_LOAD_SHARE_EXECUTION_ID)
                .put(String.format("%s.%s", value.f0, value.f1), value.f2);
    }

    @Override
    public void close() throws Exception {
        super.close();
        flush(buffer);
    }

    private void flush(List<Record> buffer) throws SQLException, ObDirectLoadException {
        if (CollectionUtils.isEmpty(buffer)) {
            return;
        }
        ObDirectLoadBucket bucket = new ObDirectLoadBucket();
        for (Record record : buffer) {
            if (record instanceof DataChangeRecord) {
                DataChangeRecord dataChangeRecord = (DataChangeRecord) record;
                TableInfo table = (TableInfo) dataChangeRecord.getTable();
                List<String> fieldNames = table.getFieldNames();
                ObObj[] array = new ObObj[fieldNames.size()];
                int index = 0;
                for (String fieldName : fieldNames) {
                    array[index++] = createObObj(dataChangeRecord.getFieldValue(fieldName));
                }
                bucket.addRow(array);
            }
        }
        try {
            directLoad.write(bucket);
            buffer.clear();
        } catch (Exception e) {
            throw new SQLException("DirectLoad write failed", e);
        }
    }
}
