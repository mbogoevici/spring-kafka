/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.stream;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaStreamFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * @author Marius Bogoevici
 */
public class KafkaStreamingTests {

	private static final String STREAMING_TOPIC1 = "streamingTopic";
	private static final String STREAMING_TOPIC2 = "streamingTopic2";

//	@BeforeClass
//	public static void setUp() throws Exception {
//		Map<String, Object> streamProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
//		streamProps.putAll(KafkaTestUtils.producerProps(embeddedKafka));
//	}

//	@ClassRule
//	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, STREAMING_TOPIC1,STREAMING_TOPIC2);

	@Test
	public void testBasicStreaming() throws Exception {

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(props);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(STREAMING_TOPIC1);

		props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
				props);

		Consumer<Integer, String> consumer = cf.createConsumer();
		final CountDownLatch consumerLatch = new CountDownLatch(1);
		consumer.subscribe(Collections.singleton(STREAMING_TOPIC2), new ConsumerRebalanceListener() {
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


		AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(KafkaStreamingApp.class);

		String payload = "foo"  + UUID.randomUUID().toString();
		String payload2 = "foo"  + UUID.randomUUID().toString();

		template.send(new Integer(0), payload);
		template.send(new Integer(0), payload2);

		try {
			assertThat(KafkaTestUtils.getSingleRecord(consumer, STREAMING_TOPIC2).value())
					.isEqualTo(payload.toUpperCase()  + payload2.toUpperCase());
		}
		finally {
			applicationContext.stop();
		}
	}

	
	@Configuration
	public static class KafkaStreamingApp {
		
		@Bean
		KafkaStreamFactoryBean kafkaStream() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
			props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
			return new TestKafkaStreamFactoryBean(props);
		}

	}

	private static class TestKafkaStreamFactoryBean extends KafkaStreamFactoryBean {

		public TestKafkaStreamFactoryBean(Map<String, Object> props) {
			super(props);
		}

		@Override
		protected void buildTopology(KStreamBuilder builder) {
			builder.<Integer,String>stream(STREAMING_TOPIC1)
					.map((i,s) -> new KeyValue<>(i, s.toUpperCase()))
					//.countByKey("counter")
					.reduceByKey((String value1, String value2) -> value1 + value2, TimeWindows.of("windowName", 1000))
					.toStream()
					.map((windowedId, value)->new KeyValue<>(windowedId.key(), value))
					.filter((i,s)->s.length()>40)
					//.to(Serdes.Integer(), Serdes.Long(), STREAMING_TOPIC2);
			.to(STREAMING_TOPIC2);
		}
	}
}
