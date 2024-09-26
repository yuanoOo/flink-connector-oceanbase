package com.oceanbase.connector.flink.sink.directload;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

public class DirectLoadSink implements Sink<String> {
    private OceanBaseConnectorOptions connectorOptions;

    public DirectLoadSink(OceanBaseConnectorOptions connectorOptions) {
        this.connectorOptions = connectorOptions;
    }

    @Override
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        return new DirectLoadWriter(connectorOptions);
    }
}
