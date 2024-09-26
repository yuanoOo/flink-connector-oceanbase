package com.oceanbase.connector.flink.directload;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import java.util.List;

public class DirectLoadUtils {
    public static List<Tuple3<String, String, String>> initExecutionIdFromConnOptions(
            OceanBaseConnectorOptions connectorOptions) {
        try {
            DirectLoader directLoader = buildDirectLoaderFromConnOption(connectorOptions, null);
            String executionId = directLoader.begin();
            return Lists.newArrayList(
                    Tuple3.of(
                            connectorOptions.getSchemaName(),
                            connectorOptions.getTableName(),
                            executionId));
        } catch (Exception e) {
            throw new RuntimeException("Fail to init executionId id.", e);
        }
    }

    public static DirectLoader buildDirectLoaderFromConnOption(
            OceanBaseConnectorOptions connectorOptions, String executionId) {
        try {
            return new DirectLoaderBuilder()
                    .host(connectorOptions.getDirectLoadHost())
                    .port(connectorOptions.getDirectLoadPort())
                    .user(connectorOptions.getUsername().split("@")[0])
                    .password(connectorOptions.getPassword())
                    .tenant(connectorOptions.getUsername().split("@")[1])
                    .schema(connectorOptions.getSchemaName())
                    .table(connectorOptions.getTableName())
                    .duplicateKeyAction(connectorOptions.getDirectLoadDupAction())
                    .maxErrorCount(connectorOptions.getDirectLoadMaxErrorRows())
                    .heartBeatTimeout(connectorOptions.getDirectLoadHeartbeatTimeout())
                    .heartBeatInterval(connectorOptions.getDirectLoadHeartbeatInterval())
                    .directLoadMethod(connectorOptions.getDirectLoadLoadMethod())
                    .parallel(connectorOptions.getDirectLoadParallel())
                    .executionId(executionId)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Fail to build DirectLoader.", e);
        }
    }
}
