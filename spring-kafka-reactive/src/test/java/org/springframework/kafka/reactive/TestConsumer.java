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

package org.springframework.kafka.reactive;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import reactor.core.publisher.TopicProcessor;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Marius Bogoevici
 */
public class TestConsumer {

	private static final String TEST_TOPIC = "templateTopic";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true,
			TEST_TOPIC);

	private static Consumer<Integer, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false",
				embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAllEmbeddedTopics(consumer);
	}

	@Test
	public void testConsumerAndFlux() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(TEST_TOPIC);


		final TopicProcessor<ConsumerRecord<?,?>> consumerRecordProcessor = TopicProcessor.create();

		consumerRecordProcessor.map(ConsumerRecord::value).consume(System.out::println);
		
		KafkaMessageListenerContainer<?,?> container = new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory(KafkaTestUtils.consumerProps("test1", "true", embeddedKafka)),
				TEST_TOPIC);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(ConsumerRecord record) {
				consumerRecordProcessor.onNext(record);
			}
		});

		container.start();
		template.send("foo");
		template.send("bar");
		template.send("baz");
		template.send("baq");
		template.send("raq");

	}

	@Test
	public void testReactiveConsumer() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(TEST_TOPIC);


		ConsumerFactory<Integer,String> consumerFactory = new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("test1", "true", embeddedKafka));

		Consumer<Integer, String> consumer = consumerFactory.createConsumer();
		consumer.subscribe(Collections.singletonList(TEST_TOPIC));



		KafkaPublisher<Integer,String> kafkaPublisher = new KafkaPublisher<>(consumer);

		kafkaPublisher.map(ConsumerRecord::value).consume(System.out::println);

		template.send("foo");
		template.send("bar");
		template.send("baz");
		template.send("baq");
		template.send("raq");

		// not a proper test
		System.in.read();
	}
}
