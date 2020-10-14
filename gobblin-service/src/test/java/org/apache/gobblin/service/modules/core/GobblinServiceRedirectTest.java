/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gobblin.service.modules.core;

import java.io.File;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestLiResponseException;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlJobStatusStateStoreFactory;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigClient;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;


@Test
public class GobblinServiceRedirectTest {

  private static final Logger logger = LoggerFactory.getLogger(GobblinServiceRedirectTest.class);

  private static final String QUARTZ_INSTANCE_NAME = "org.quartz.scheduler.instanceName";
  private static final String QUARTZ_THREAD_POOL_COUNT = "org.quartz.threadPool.threadCount";

  private static final String COMMON_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreCommon/";

  private static final String NODE_1_SERVICE_WORK_DIR = "/tmp/serviceWorkDirNode1/";
  private static final String NODE_1_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreNode1/";
  private static final String NODE_1_TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCoreNode1/topologyTestSpecStoreNode1";
  private static final String NODE_1_FLOW_SPEC_STORE_DIR = "/tmp/serviceCoreCommon/flowTestSpecStore";
  private static final String NODE_1_JOB_STATUS_STATE_STORE_DIR = "/tmp/serviceCoreNode1/fsJobStatusRetriever";

  private static final String NODE_2_SERVICE_WORK_DIR = "/tmp/serviceWorkDirNode2/";
  private static final String NODE_2_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreNode2/";
  private static final String NODE_2_TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCoreNode2/topologyTestSpecStoreNode2";
  private static final String NODE_2_FLOW_SPEC_STORE_DIR = "/tmp/serviceCoreCommon/flowTestSpecStore";
  private static final String NODE_2_JOB_STATUS_STATE_STORE_DIR = "/tmp/serviceCoreNode2/fsJobStatusRetriever";

  private static final String TEST_HELIX_CLUSTER_NAME = "testGobblinServiceCluster";

  private static final String TEST_GROUP_NAME_1 = "testGroup1";
  private static final String TEST_FLOW_NAME_1 = "testFlow1";
  private static final String TEST_SCHEDULE_1 = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI_1 = "FS:///templates/test.template";

  private static final String TEST_GROUP_NAME_2 = "testGroup2";
  private static final String TEST_FLOW_NAME_2 = "testFlow2";
  private static final String TEST_SCHEDULE_2 = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI_2 = "FS:///templates/test.template";

  private static final String TEST_GOBBLIN_EXECUTOR_NAME = "testGobblinExecutor";
  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_SINK_NAME = "testSink";

  private static final String PORT1 = "1000";
  private static final String PORT2 = "2000";
  private static final String PREFIX = "https://";
  private static final String SERVICE_NAME = "gobblinServiceTest";

  private GobblinServiceManager node1GobblinServiceManager;
  private FlowConfigClient node1FlowConfigClient;

  private GobblinServiceManager node2GobblinServiceManager;
  private FlowConfigClient node2FlowConfigClient;

  private TestingServer testingZKServer;

  private Properties node1ServiceCoreProperties;
  private Properties node2ServiceCoreProperties;

  @BeforeClass
  public void setup() throws Exception {
    // Clean up common Flow Spec Dir
    cleanUpDir(COMMON_SPEC_STORE_PARENT_DIR);

    // Clean up work dir for Node 1
    cleanUpDir(NODE_1_SERVICE_WORK_DIR);
    cleanUpDir(NODE_1_SPEC_STORE_PARENT_DIR);

    // Clean up work dir for Node 2
    cleanUpDir(NODE_2_SERVICE_WORK_DIR);
    cleanUpDir(NODE_2_SPEC_STORE_PARENT_DIR);

    // Use a random ZK port
    this.testingZKServer = new TestingServer(-1);
    logger.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());
    HelixUtils.createGobblinHelixCluster(testingZKServer.getConnectString(), TEST_HELIX_CLUSTER_NAME);

    ITestMetastoreDatabase testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    Properties commonServiceCoreProperties = new Properties();
    commonServiceCoreProperties.put(ServiceConfigKeys.ZK_CONNECTION_STRING_KEY, testingZKServer.getConnectString());
    commonServiceCoreProperties.put(ServiceConfigKeys.HELIX_CLUSTER_NAME_KEY, TEST_HELIX_CLUSTER_NAME);
    commonServiceCoreProperties.put(ServiceConfigKeys.HELIX_INSTANCE_NAME_KEY, "GaaS_" + UUID.randomUUID().toString());
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY , TEST_GOBBLIN_EXECUTOR_NAME);
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".description",
        "StandaloneTestExecutor");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".version",
        "1");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".uri",
        "gobblinExecutor");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".specExecutorInstance",
        "org.gobblin.service.InMemorySpecExecutor");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".specExecInstance.capabilities",
        TEST_SOURCE_NAME + ":" + TEST_SINK_NAME);
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_USER_KEY, "testUser");
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "testPassword");
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testMetastoreDatabase.getJdbcUrl());
    commonServiceCoreProperties.put("zookeeper.connect", testingZKServer.getConnectString());
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_FACTORY_CLASS_KEY, MysqlJobStatusStateStoreFactory.class.getName());
    commonServiceCoreProperties.put(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY, false);

    commonServiceCoreProperties.put(ServiceConfigKeys.FORCE_LEADER, true);
    commonServiceCoreProperties.put(ServiceConfigKeys.SERVICE_URL_PREFIX, PREFIX);
    commonServiceCoreProperties.put(ServiceConfigKeys.SERVICE_NAME, SERVICE_NAME);

    node1ServiceCoreProperties = new Properties();
    node1ServiceCoreProperties.putAll(commonServiceCoreProperties);
    node1ServiceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, NODE_1_TOPOLOGY_SPEC_STORE_DIR);
    node1ServiceCoreProperties.put(FlowCatalog.FLOWSPEC_STORE_DIR_KEY, NODE_1_FLOW_SPEC_STORE_DIR);
    node1ServiceCoreProperties.put(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, NODE_1_JOB_STATUS_STATE_STORE_DIR);
    node1ServiceCoreProperties.put(QUARTZ_INSTANCE_NAME, "QuartzScheduler1");
    node1ServiceCoreProperties.put(QUARTZ_THREAD_POOL_COUNT, 3);
    node1ServiceCoreProperties.put(ServiceConfigKeys.SERVICE_PORT, PORT1);

    node2ServiceCoreProperties = new Properties();
    node2ServiceCoreProperties.putAll(commonServiceCoreProperties);
    node2ServiceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, NODE_2_TOPOLOGY_SPEC_STORE_DIR);
    node2ServiceCoreProperties.put(FlowCatalog.FLOWSPEC_STORE_DIR_KEY, NODE_2_FLOW_SPEC_STORE_DIR);
    node2ServiceCoreProperties.put(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, NODE_2_JOB_STATUS_STATE_STORE_DIR);
    node2ServiceCoreProperties.put(QUARTZ_INSTANCE_NAME, "QuartzScheduler2");
    node2ServiceCoreProperties.put(QUARTZ_THREAD_POOL_COUNT, 3);
    node2ServiceCoreProperties.put(ServiceConfigKeys.SERVICE_PORT, PORT2);

    // Start Node 1
    this.node1GobblinServiceManager = new GobblinServiceManager("CoreService1", "1",
        ConfigUtils.propertiesToConfig(node1ServiceCoreProperties), Optional.of(new Path(NODE_1_SERVICE_WORK_DIR)));
    this.node1GobblinServiceManager.start();

    // Start Node 2
    this.node2GobblinServiceManager = new GobblinServiceManager("CoreService2", "2",
        ConfigUtils.propertiesToConfig(node2ServiceCoreProperties), Optional.of(new Path(NODE_2_SERVICE_WORK_DIR)));
    this.node2GobblinServiceManager.start();

    // Initialize Node 1 Client
    Map<String, String> transportClientProperties = Maps.newHashMap();
    transportClientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "10000");
    this.node1FlowConfigClient = new FlowConfigClient(String.format("http://localhost:%s/",
        this.node1GobblinServiceManager.restliServer.getPort()), transportClientProperties);

    // Initialize Node 2 Client
    this.node2FlowConfigClient = new FlowConfigClient(String.format("http://localhost:%s/",
        this.node2GobblinServiceManager.restliServer.getPort()), transportClientProperties);
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterClass
  public void cleanUp() throws Exception {
    // Shutdown Node 1
    try {
      logger.info("+++++++++++++++++++ start shutdown noad1");
      this.node1GobblinServiceManager.stop();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Node 1 of Gobblin Service", e);
    }

    // Shutdown Node 2
    try {
      logger.info("+++++++++++++++++++ start shutdown noad2");
      this.node2GobblinServiceManager.stop();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Node 2 of Gobblin Service", e);
    }

    // Stop Zookeeper
    try {
      this.testingZKServer.close();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Testing Zookeeper", e);
    }

    // Cleanup Node 1
    try {
      cleanUpDir(NODE_1_SERVICE_WORK_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 1 Work Dir");
    }
    try {
      cleanUpDir(NODE_1_SPEC_STORE_PARENT_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 1 Spec Store Parent Dir");
    }

    // Cleanup Node 2
    try {
      cleanUpDir(NODE_2_SERVICE_WORK_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 2 Work Dir");
    }
    try {
      cleanUpDir(NODE_2_SPEC_STORE_PARENT_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 2 Spec Store Parent Dir");
    }

    cleanUpDir(COMMON_SPEC_STORE_PARENT_DIR);
  }

  @Test
  public void testCreate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig1 = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1))
        .setTemplateUris(TEST_TEMPLATE_URI_1).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_1).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));
    FlowConfig flowConfig2 = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_2).setFlowName(TEST_FLOW_NAME_2))
        .setTemplateUris(TEST_TEMPLATE_URI_2).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_2).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    GobblinServiceManager leader;
    FlowConfigClient leaderClient;
    FlowConfigClient slaveClient;
    if (this.node1GobblinServiceManager.isLeader()) {
      leader = this.node1GobblinServiceManager;
      leaderClient = this.node1FlowConfigClient;
      slaveClient = this.node2FlowConfigClient;
    } else {
      leader = this.node2GobblinServiceManager;
      leaderClient = this.node2FlowConfigClient;
      slaveClient = this.node1FlowConfigClient;
    }

    // Try create on leader, should be successful
    leaderClient.createFlowConfig(flowConfig1);

    // Try create on slave, should throw an error with leader URL
    try {
      slaveClient.createFlowConfig(flowConfig2);
    } catch (RestLiResponseException e) {
      Assert.assertTrue(e.hasErrorDetails());
      Assert.assertTrue(e.getErrorDetails().containsKey(ServiceConfigKeys.LEADER_URL));
      String expectedUrl = PREFIX + InetAddress.getLocalHost().getHostName() + ":" + leader.restliServer.getPort() + "/" + SERVICE_NAME;
      Assert.assertEquals(e.getErrorDetails().get(ServiceConfigKeys.LEADER_URL), expectedUrl);
      return;
    }

    throw new RuntimeException("Slave should have thrown an error");
  }
}