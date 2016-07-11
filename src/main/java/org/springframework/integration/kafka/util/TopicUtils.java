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

package org.springframework.integration.kafka.util;

import java.util.List;
import java.util.Properties;

import kafka.admin.RackAwareMode;
import kafka.common.LeaderNotAvailableException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.common.requests.MetadataResponse;
import org.springframework.integration.kafka.core.TopicNotFoundException;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.CompositeRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.policy.TimeoutRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.Map;
import scala.collection.Seq;

/**
 * Utilities for interacting with Kafka topics
 *
 * @author Marius Bogoevici
 */
public class TopicUtils {

	private static Log log = LogFactory.getLog(TopicUtils.class);

	public static final int METADATA_VERIFICATION_TIMEOUT = 5000;

	public static final int METADATA_VERIFICATION_RETRY_ATTEMPTS = 10;

	public static final double METADATA_VERIFICATION_RETRY_BACKOFF_MULTIPLIER = 1.5;

	public static final int METADATA_VERIFICATION_RETRY_INITIAL_INTERVAL = 100;

	public static final int METADATA_VERIFICATION_MAX_INTERVAL = 1000;

	/**
	 * Creates a topic in Kafka or validates that it exists with the requested number of partitions,
	 * and returns only after the topic has been fully created
	 * @param zkAddress the address of the Kafka ZooKeeper instance
	 * @param topicName the name of the topic
	 * @param numPartitions the number of partitions
	 * @param replicationFactor the replication factor
	 * @return {@link TopicMetadata} information for the topic
	 */
	public static MetadataResponse.TopicMetadata ensureTopicCreated(final String zkAddress, final String topicName,
			final int numPartitions, int replicationFactor) {

		final int sessionTimeoutMs = 10000;
		final int connectionTimeoutMs = 10000;
		final ZkClient zkClient =
				new ZkClient(zkAddress, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
		final ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkAddress), false);
		try {
			// The following is basically copy/paste from AdminUtils.createTopic() with
			// createOrUpdateTopicPartitionAssignmentPathInZK(..., update=true)
			Properties topicConfig = new Properties();
			Seq<Object> brokerList = zkUtils.getSortedBrokerList();
			scala.collection.Map<Object, Seq<Object>> replicaAssignment = AdminUtils.assignReplicasToBrokers(
					AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Disabled$.MODULE$, Option.apply(brokerList)),
					numPartitions, replicationFactor, -1, -1);
			return ensureTopicCreated(zkUtils, topicName, numPartitions, topicConfig, replicaAssignment);

		} finally {
			zkClient.close();
		}
	}

	/**
	 * Creates a topic in Kafka and returns only after the topic has been fully and an produce metadata.
	 * @param zkUtils an open {@link ZkUtils} connection to Zookeeper
	 * @param topicName the name of the topic
	 * @param numPartitions the number of partitions for the topic
	 * @param topicConfig additional topic configuration properties
	 * @param replicaAssignment the mapping of partitions to broker
	 * @return {@link TopicMetadata} information for the topic
	 */
	public static MetadataResponse.TopicMetadata ensureTopicCreated(final ZkUtils zkUtils, final String topicName,
			final int numPartitions, Properties topicConfig, Map<Object, Seq<Object>> replicaAssignment) {

		AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topicName, replicaAssignment, topicConfig,
				true);

		RetryTemplate retryTemplate = new RetryTemplate();

		CompositeRetryPolicy policy = new CompositeRetryPolicy();
		TimeoutRetryPolicy timeoutRetryPolicy = new TimeoutRetryPolicy();
		timeoutRetryPolicy.setTimeout(METADATA_VERIFICATION_TIMEOUT);
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
		simpleRetryPolicy.setMaxAttempts(METADATA_VERIFICATION_RETRY_ATTEMPTS);
		policy.setPolicies(new RetryPolicy[] { timeoutRetryPolicy, simpleRetryPolicy });
		retryTemplate.setRetryPolicy(policy);

		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(METADATA_VERIFICATION_RETRY_INITIAL_INTERVAL);
		backOffPolicy.setMultiplier(METADATA_VERIFICATION_RETRY_BACKOFF_MULTIPLIER);
		backOffPolicy.setMaxInterval(METADATA_VERIFICATION_MAX_INTERVAL);
		retryTemplate.setBackOffPolicy(backOffPolicy);

		try {
			return retryTemplate.execute(new RetryCallback<MetadataResponse.TopicMetadata, Exception>() {
				@Override
				public MetadataResponse.TopicMetadata doWithRetry(RetryContext context) throws Exception {
					MetadataResponse.TopicMetadata topicMetadata =
							AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
					if (topicMetadata.error().code() != ErrorMapping.NoError() || !topicName
							.equals(topicMetadata.topic())) {
						// downcast to Exception because that's what the error throws
						throw (Exception) ErrorMapping.exceptionFor(topicMetadata.error().code());
					}
					List<MetadataResponse.PartitionMetadata> partitionMetadatas = (topicMetadata).partitionMetadata();
					if (partitionMetadatas.size() != numPartitions) {
						throw new IllegalStateException("The number of expected partitions was: " +
								numPartitions + ", but " + partitionMetadatas.size() + " have been found instead");
					}
					for (MetadataResponse.PartitionMetadata partitionMetadata : partitionMetadatas) {
						if (partitionMetadata.error().code() != ErrorMapping.NoError()) {
							throw (Exception) ErrorMapping.exceptionFor(partitionMetadata.error().code());
						}
						if (partitionMetadata.leader() == null) {
							throw new LeaderNotAvailableException();
						}
					}
					return topicMetadata;
				}
			});
		} catch (Exception e) {
			log.error(String.format("Cannot retrieve metadata for topic '%s'", topicName), e);
			throw new TopicNotFoundException(topicName);
		}
	}

}
