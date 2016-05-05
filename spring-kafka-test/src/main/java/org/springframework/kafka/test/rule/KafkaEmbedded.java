/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.rule;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ServerSocketFactory;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.ListIterate;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.junit.rules.ExternalResource;

import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * The {@link KafkaRule} implementation for the embedded Kafka Broker and Zookeeper.
 *
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class KafkaEmbedded extends ExternalResource implements KafkaRule {

	public static final long METADATA_PROPAGATION_TIMEOUT = 10000L;

	private final int count;

	private final boolean controlledShutdown;

	private final String[] topics;

	private final int partitionsPerTopic;

	private List<KafkaServer> kafkaServers;

	private EmbeddedZookeeper zookeeper;

	private ZkClient zookeeperClient;

	private String zkConnect;

	public KafkaEmbedded(int count) {
		this(count, false);
	}

	/**
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param topics the topics to create (2 partitions per).
	 */
	public KafkaEmbedded(int count, boolean controlledShutdown, String... topics) {
		this(count, controlledShutdown, 2, topics);
	}

	/**
	 *
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public KafkaEmbedded(int count, boolean controlledShutdown, int partitions, String... topics) {
		this.count = count;
		this.controlledShutdown = controlledShutdown;
		if (topics != null) {
			this.topics = topics;
		}
		else {
			this.topics = new String[0];
		}
		this.partitionsPerTopic = partitions;
	}

	@Override
	protected void before() throws Exception { //NOSONAR
		startZookeeper();
		int zkConnectionTimeout = 6000;
		int zkSessionTimeout = 6000;

		this.zkConnect = "127.0.0.1:" + this.zookeeper.port();
		this.zookeeperClient = new ZkClient(this.zkConnect, zkSessionTimeout, zkConnectionTimeout,
				ZKStringSerializer$.MODULE$);
		this.kafkaServers = new ArrayList<>();
		for (int i = 0; i < this.count; i++) {
			ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0);
			int randomPort = ss.getLocalPort();
			ss.close();
			Properties brokerConfigProperties = TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
					true, randomPort,
					scala.Option.<SecurityProtocol>apply(null),
					scala.Option.<File>apply(null), scala.Option.<Properties>apply(null),
					true, false, 0, false, 0, false, 0, scala.Option.<String>apply(null));
			brokerConfigProperties.setProperty("replica.socket.timeout.ms", "1000");
			brokerConfigProperties.setProperty("controller.socket.timeout.ms", "1000");
			brokerConfigProperties.setProperty("offsets.topic.replication.factor", "1");
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties, false), SystemTime$.MODULE$);
			this.kafkaServers.add(server);
		}
		ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
		Properties props = new Properties();
		for (String topic : this.topics) {
			AdminUtils.createTopic(zkUtils, topic, this.partitionsPerTopic, this.count, props, null);
		}
	}

	@Override
	protected void after() {
		for (KafkaServer kafkaServer : this.kafkaServers) {
			try {
				if (kafkaServer.brokerState().currentState() != (NotRunning.state())) {
					kafkaServer.shutdown();
					kafkaServer.awaitShutdown();
				}
			}
			catch (Exception e) {
				// do nothing
			}
			try {
				//CoreUtils.rm(kafkaServer.config().logDirs());
			}
			catch (Exception e) {
				// do nothing
			}
		}
		try {
			this.zookeeperClient.close();
		}
		catch (ZkInterruptedException e) {
			// do nothing
		}
		try {
			this.zookeeper.shutdown();
		}
		catch (Exception e) {
			// do nothing
		}
	}

	@Override
	public List<KafkaServer> getKafkaServers() {
		return this.kafkaServers;
	}

	public KafkaServer getKafkaServer(int id) {
		return this.kafkaServers.get(id);
	}

	public EmbeddedZookeeper getZookeeper() {
		return this.zookeeper;
	}

	@Override
	public ZkClient getZkClient() {
		return this.zookeeperClient;
	}

	@Override
	public String getZookeeperConnectionString() {
		return this.zkConnect;
	}

	public BrokerAddress getBrokerAddress(int i) {
		KafkaServer kafkaServer = this.kafkaServers.get(i);
		return new BrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port());
	}

	@Override
	public BrokerAddress[] getBrokerAddresses() {
		return ListIterate.collect(this.kafkaServers,
				new Function<KafkaServer, BrokerAddress>() {

					@Override
					public BrokerAddress valueOf(KafkaServer kafkaServer) {
						return new BrokerAddress("127.0.0.1", kafkaServer.config().port());
					}

				})
				.toArray(new BrokerAddress[this.kafkaServers.size()]);
	}

	@Override
	public int getPartitionsPerTopic() {
		return this.partitionsPerTopic;
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
		this.zookeeper = new EmbeddedZookeeper();
	}


	public void restart(final int index) throws Exception { //NOSONAR

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
			public Void doWithRetry(RetryContext context) throws Exception { //NOSONAR
				KafkaEmbedded.this.kafkaServers.get(index).startup();
				return null;
			}
		});
	}


	@Override
	public String getBrokersAsString() {
		return FastList.newList(Arrays.asList(getBrokerAddresses()))
				.collect(new Function<BrokerAddress, String>() {

					@Override
					public String valueOf(BrokerAddress object) {
						return object.getHost() + ":" + object.getPort();
					}

				})
				.makeString(",");
	}

	@Override
	public boolean isEmbedded() {
		return true;
	}

	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) throws Exception {
		final CountDownLatch consumerLatch = new CountDownLatch(1);
		consumer.subscribe(Arrays.asList(this.topics), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				consumerLatch.countDown();
			}

		});
		consumer.poll(0); // force assignment
		assertThat(consumerLatch.await(30, TimeUnit.SECONDS))
			.as("Failed to be assigned partitions from the embedded topics")
			.isTrue();
	}

}
