package com.oceanbase.connector.flink.sink.directload;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

public class DirectLoadWriter implements SinkWriter<String> {

    private final OceanBaseConnectorOptions connectorOptions;
    private DirectLoader directLoader;
    private volatile String executionId;

    public DirectLoadWriter(OceanBaseConnectorOptions connectorOptions) {
        this.connectorOptions = connectorOptions;
    }

    @Override
    public void write(String element, Context context) throws IOException, InterruptedException {
        executionId = element;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            try {
                Preconditions.checkState(
                        !StringUtils.isNullOrWhitespaceOnly(executionId),
                        "Execution id should not be NULL.");
                directLoader =
                        DirectLoadUtils.buildDirectLoaderFromConnOption(
                                connectorOptions, executionId);
                directLoader.begin();
                ObDirectLoadBucket bucket = new ObDirectLoadBucket();
                ObObj[] array = new ObObj[6];
                List<ObObj> xxx = Arrays.asList(createObObj(8), createObObj(new Timestamp(System.currentTimeMillis())),
                    createObObj("xxx"), createObObj(2.22), createObObj(11), createObObj(true));
                bucket.addRow(xxx);
                directLoader.write(bucket);
                directLoader.commit();
            } catch (Exception e) {
                throw new RuntimeException("DirectLoad job commit failed.", e);
            } finally {
                if (Objects.nonNull(directLoader)) {
                    directLoader.close();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {}
}
