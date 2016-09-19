/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.rule;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.ListIterate;

import kafka.admin.AdminUtils$;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.BrokerEndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.junit.rules.ExternalResource;

import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.Set;

import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Marius Bogoevici
 * @author Artem Bilan
 */
@SuppressWarnings("serial")
public class KafkaEmbedded extends ExternalResource implements KafkaRule {

    public static final long METADATA_PROPAGATION_TIMEOUT = 10000L;

    private int count;

    private boolean controlledShutdown;

    private List<Integer> kafkaPorts;

    private List<KafkaServer> kafkaServers;

    private EmbeddedZookeeper zookeeper;

    private ZkClient zookeeperClient;

    private ZkUtils zkUtils;

    @SuppressWarnings("unchecked")
    public KafkaEmbedded(int count, boolean controlledShutdown) {
        this.count = count;
        this.controlledShutdown = controlledShutdown;

        this.kafkaPorts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            kafkaPorts.add(TestUtils.RandomPort());
        }
    }

    public KafkaEmbedded(int count) {
        this(count, false);
    }

    @Override
    protected void before() throws Throwable {
        startZookeeper();
        int zkConnectionTimeout = 6000;
        int zkSessionTimeout = 6000;
        zookeeperClient = new ZkClient(TestUtils.MockZkConnect(), zkSessionTimeout, zkConnectionTimeout,
                ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zookeeperClient, new ZkConnection(getZookeeperConnectionString()), true);

        kafkaServers = new ArrayList<KafkaServer>();
        for (int i = 0; i < count; i++) {
            Properties brokerConfigProperties = TestUtils
                    .createBrokerConfig(i, getZookeeperConnectionString(), controlledShutdown, true,
                            TestUtils.RandomPort(), Option.<SecurityProtocol>empty(), Option.<File>empty(), true, false,
                            TestUtils.RandomPort(), false, TestUtils.RandomPort(), false, TestUtils.RandomPort());
            brokerConfigProperties.setProperty("replica.socket.timeout.ms", "1000");
            brokerConfigProperties.setProperty("controller.socket.timeout.ms", "1000");
            brokerConfigProperties.setProperty("offsets.topic.replication.factor", "1");
            KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), SystemTime$.MODULE$);
            kafkaServers.add(server);
        }
    }

    @Override
    protected void after() {
        for (KafkaServer kafkaServer : kafkaServers) {
            try {
                if (kafkaServer.brokerState().currentState() != (NotRunning.state())) {
                    kafkaServer.shutdown();
                    kafkaServer.awaitShutdown();
                }
            } catch (Exception e) {
                // do nothing
            }
            try {
                //TestUtils.rm(kafkaServer.config().logDirs());
            } catch (Exception e) {
                // do nothing
            }
        }
        try {
            zookeeperClient.close();
        } catch (ZkInterruptedException e) {
            // do nothing
        }
        try {
            zookeeper.shutdown();
        } catch (Exception e) {
            // do nothing
        }
    }

    public List<KafkaServer> getKafkaServers() {
        return kafkaServers;
    }

    public KafkaServer getKafkaServer(int id) {
        return kafkaServers.get(id);
    }

    public EmbeddedZookeeper getZookeeper() {
        return zookeeper;
    }

    @Override
    public ZkClient getZkClient() {
        return zookeeperClient;
    }

    @Override
    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    @Override
    public String getZookeeperConnectionString() {
        return TestUtils.MockZkConnect();
    }

    public BrokerAddress getBrokerAddress(int i) {
        KafkaServer kafkaServer = this.kafkaServers.get(i);
        return new BrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port());
    }

    @Override
    public BrokerAddress[] getBrokerAddresses() {
        return ListIterate.collect(this.kafkaServers, new Function<KafkaServer, BrokerAddress>() {

            @Override
            public BrokerAddress valueOf(KafkaServer kafkaServer) {
                return new BrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port());
            }

        }).toArray(new BrokerAddress[this.kafkaServers.size()]);
    }

    public void bounce(BrokerAddress brokerAddress) {
        for (KafkaServer kafkaServer : getKafkaServers()) {
            if (brokerAddress.equals(new BrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port()))) {
                kafkaServer.shutdown();
                kafkaServer.awaitShutdown();
            }
        }
    }

    public void startZookeeper() {
        zookeeper = new EmbeddedZookeeper();
    }

    public void bounce(int index, boolean waitForPropagation) {
        kafkaServers.get(index).shutdown();
        if (waitForPropagation) {
            long initialTime = System.currentTimeMillis();
            boolean canExit = false;
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
                canExit = true;
                Map<String, Properties> topicProperties = AdminUtils$.MODULE$.fetchAllTopicConfigs(getZkUtils());
                Set<TopicMetadata> topicMetadatas =
                        AdminUtils$.MODULE$.fetchTopicMetadataFromZk(topicProperties.keySet(), getZkUtils());
                for (TopicMetadata topicMetadata : JavaConversions.asJavaCollection(topicMetadatas)) {
                    if (Errors.forCode(topicMetadata.errorCode()).exception() == null) {
                        for (PartitionMetadata partitionMetadata : JavaConversions
                                .asJavaCollection(topicMetadata.partitionsMetadata())) {
                            Collection<BrokerEndPoint> inSyncReplicas =
                                    JavaConversions.asJavaCollection(partitionMetadata.isr());
                            for (BrokerEndPoint broker : inSyncReplicas) {
                                if (broker.id() == index) {
                                    canExit = false;
                                }
                            }
                        }
                    }
                }
            } while (!canExit && (System.currentTimeMillis() - initialTime < METADATA_PROPAGATION_TIMEOUT));
        }

    }

    public void bounce(int index) {
        bounce(index, true);
    }

    public void restart(final int index) throws Exception {

        // retry restarting repeatedly, first attempts may fail

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10,
                Collections.<Class<? extends Throwable>, Boolean>singletonMap(Exception.class, true));

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(100);
        backOffPolicy.setMaxInterval(1000);
        backOffPolicy.setMultiplier(2);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        retryTemplate.execute(new RetryCallback<Void, Exception>() {
            @Override
            public Void doWithRetry(RetryContext context) throws Exception {
                kafkaServers.get(index).startup();
                return null;
            }
        });
    }

    public void waitUntilSynced(String topic, int brokerId) {
        long initialTime = System.currentTimeMillis();
        boolean canExit = false;
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
            canExit = true;
            TopicMetadata topicMetadata = AdminUtils$.MODULE$.fetchTopicMetadataFromZk(topic, getZkUtils());
            if (Errors.forCode(topicMetadata.errorCode()).exception() == null) {
                for (PartitionMetadata partitionMetadata : JavaConversions
                        .asJavaCollection(topicMetadata.partitionsMetadata())) {
                    Collection<BrokerEndPoint> isr = JavaConversions.asJavaCollection(partitionMetadata.isr());
                    boolean containsIndex = false;
                    for (BrokerEndPoint broker : isr) {
                        if (broker.id() == brokerId) {
                            containsIndex = true;
                        }
                    }
                    if (!containsIndex) {
                        canExit = false;
                    }

                }
            }
        } while (!canExit && (System.currentTimeMillis() - initialTime < METADATA_PROPAGATION_TIMEOUT));
    }

    @Override
    public String getBrokersAsString() {
        return FastList.newList(Arrays.asList(getBrokerAddresses())).collect(new Function<BrokerAddress, String>() {

            @Override
            public String valueOf(BrokerAddress object) {
                return object.getHost() + ":" + object.getPort();
            }

        }).makeString(",");
    }

    @Override
    public boolean isEmbedded() {
        return true;
    }

}
