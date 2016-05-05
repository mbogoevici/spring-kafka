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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaStreamFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Marius Bogoevici
 */
public class KafkaStreamingTests {

	private static final String STREAMING_TOPIC1 = "streamingTopic";
	private static final String STREAMING_TOPIC2 = "streamingTopic2";

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> streamProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
		streamProps.putAll(KafkaTestUtils.producerProps(embeddedKafka));
	}

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, STREAMING_TOPIC1,STREAMING_TOPIC2);

	@Test
	public void testBasicStreaming() throws Exception {

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(STREAMING_TOPIC1);

		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
				KafkaTestUtils.consumerProps("testT2", "false", embeddedKafka));
		Consumer<Integer, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAllEmbeddedTopics(consumer);

		template.send("foo");

		assertThat(KafkaTestUtils.getSingleRecord(consumer, STREAMING_TOPIC2)).has(value("foo"));
	}
	
	@Configuration
	public static class KafkaStreamingApp {
		
		@Bean
		KafkaStreamFactoryBean kafkaStream() {
			Map<String, Object> streamProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
			streamProps.putAll(KafkaTestUtils.producerProps(embeddedKafka));
			return new KafkaStreamFactoryBean(streamProps) {
				@Override
				protected void buildTopology(KStreamBuilder builder) {
					builder.stream(STREAMING_TOPIC1).to(STREAMING_TOPIC2);
				}
			};
		}
		
	}

}
