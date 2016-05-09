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

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

/**
 * @author Marius Bogoevici
 */
public class KafkaPublisher<K,V> extends Flux<ConsumerRecord<K,V>> {

	private final Consumer<K,V> consumer;

	private Runnable deliverableRunnable;

	private volatile Subscription subscription;

	public KafkaPublisher(Consumer<K, V> consumer) {
		this.consumer = consumer;
	}

	private AtomicLong requested = new AtomicLong(0);

	private boolean running = false;

	@Override
	public synchronized void subscribe(final Subscriber<? super ConsumerRecord<K, V>> subscriber) {
		if (subscription != null) {
			subscriber.onError(new IllegalStateException("A subscription already exists"));
		}
		subscription = new KafkaSubscription(subscriber);
		subscriber.onSubscribe(subscription);

	}

	private class KafkaSubscription implements Subscription {

		private final Subscriber<? super ConsumerRecord<K, V>> subscriber;

		public KafkaSubscription(Subscriber<? super ConsumerRecord<K, V>> subscriber) {
			this.subscriber = subscriber;
		}

		@Override
		public void request(long n) {
			if (deliverableRunnable == null) {
				deliverableRunnable = new Runnable() {

					@Override
					public void run() {
						while (requested.get() > 0) {
							System.out.println("Polling requests ...");
							System.out.println("Polling requests ... found " + requested);
							long delivered = 0;
							while (delivered < requested.get()) {
								ConsumerRecords<K, V> consumerRecords = consumer.poll(1000);
								for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
									subscriber.onNext(consumerRecord);
									delivered++;
								}
							}
						}
						subscriber.onComplete();
					}
				};
				Executors.newSingleThreadExecutor().execute(deliverableRunnable);
			}
			requested.addAndGet(n);
		}

		@Override
		public void cancel() {
			requested.set(0);
		}
	}
}
