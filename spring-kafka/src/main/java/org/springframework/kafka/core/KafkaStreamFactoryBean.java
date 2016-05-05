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

package org.springframework.kafka.core;

import java.util.Map;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;

/**
 * @author Marius Bogoevici
 */
public abstract class KafkaStreamFactoryBean extends AbstractFactoryBean<KafkaStreams> implements SmartLifecycle  {

	private final StreamsConfig streamsConfig;

	private volatile boolean running = false;
	
	public KafkaStreamFactoryBean(Map<String,Object> config) {
		streamsConfig = new StreamsConfig(config);
	}
	
	protected abstract void buildTopology(KStreamBuilder builder);

	@Override
	public Class<?> getObjectType() {
		return KafkaStreams.class;
	}

	@Override
	protected KafkaStreams createInstance() throws Exception {
		KStreamBuilder streamBuilder = new KStreamBuilder();
		buildTopology(streamBuilder);
		return new KafkaStreams(streamBuilder, streamsConfig);
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public synchronized void start() {
		if (!running) {
			try {
				getObject().start();
				running = true;
			}
			catch (Exception e) {
				throw new KafkaException("Could not start stream:", e);
			}
		}
	}

	@Override
	public synchronized void stop() {
		if (running) {
			try {
				getObject().close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				running = false;
			}
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return running;
	}

	@Override
	public int getPhase() {
		return 0;
	}
}
