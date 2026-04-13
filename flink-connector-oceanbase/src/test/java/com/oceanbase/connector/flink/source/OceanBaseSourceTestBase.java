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

import com.oceanbase.connector.flink.OceanBaseMySQLTestBase;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;

/** Base class for OceanBase source failover tests with MiniCluster support. */
public abstract class OceanBaseSourceTestBase extends OceanBaseMySQLTestBase {

    protected static final int DEFAULT_PARALLELISM = 4;

    protected static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    protected enum FailoverType {
        TM,
        JM,
        NONE
    }

    protected static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    protected static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    protected static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        ensureJmLeaderServiceExists(haLeadershipControl, jobId);
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        ensureJmLeaderServiceExists(haLeadershipControl, jobId);
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void ensureJmLeaderServiceExists(
            HaLeadershipControl leadershipControl, JobID jobId) throws Exception {
        EmbeddedHaServices control = (EmbeddedHaServices) leadershipControl;
        control.getJobManagerLeaderElection(jobId).close();
    }

    protected static void waitForSinkSize(String sinkName, int minSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < minSize) {
            Thread.sleep(100);
        }
    }

    protected static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                return 0;
            }
        }
    }
}
