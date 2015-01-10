/*
 * Copyright 2014 the original author or authors.
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


package org.springframework.integration.kafka.listener;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.integration.kafka.util.MessageUtils.decodeKey;
import static org.springframework.integration.kafka.util.MessageUtils.decodePayload;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gs.collections.api.multimap.list.MutableListMultimap;
import com.gs.collections.impl.multimap.list.SynchronizedPutFastListMultimap;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.Partition;

/**
 * @author Marius Bogoevici
 */

public class TestMultiBroker5ReplicatedWithBounce extends AbstractMessageListenerContainerTest {

	@Rule
	public KafkaEmbeddedBrokerRule kafkaEmbeddedBrokerRule = new KafkaEmbeddedBrokerRule(5);

	@Override
	public KafkaEmbeddedBrokerRule getKafkaRule() {
		return kafkaEmbeddedBrokerRule;
	}

	@Test
	public void testLowVolumeLowConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 3);

		ConnectionFactory connectionFactory = getKafkaBrokerConnectionFactory();
		ArrayList<Partition> readPartitions = new ArrayList<Partition>();
		for (int i = 0; i < 5; i++) {
				readPartitions.add(new Partition(TEST_TOPIC, i));
		}
		final KafkaMessageListenerContainer kafkaMessageListenerContainer = new KafkaMessageListenerContainer(connectionFactory, readPartitions.toArray(new Partition[readPartitions.size()]));
		kafkaMessageListenerContainer.setMaxFetchSizeInBytes(100);
		kafkaMessageListenerContainer.setConcurrency(2);

		final int expectedMessageCount = 100;

		createStringProducer(0).send(createMessages(100));


		final MutableListMultimap<Integer,KeyedMessageWithOffset> receivedData = new SynchronizedPutFastListMultimap<Integer, KeyedMessageWithOffset>();
		final CountDownLatch latch = new CountDownLatch(expectedMessageCount);
		kafkaMessageListenerContainer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(KafkaMessage message) {
				StringDecoder decoder = new StringDecoder(new VerifiableProperties());
				receivedData.put(message.getMetadata().getPartition().getId(),new KeyedMessageWithOffset(decodeKey(message, decoder), decodePayload(message, decoder), message.getMetadata().getOffset(), Thread.currentThread().getName(), message.getMetadata().getPartition().getId()));
				latch.countDown();
			}
		});

		kafkaMessageListenerContainer.start();

		for (int i = 0; i < 5; i++) {
			if (i % 2 == 0) {
				kafkaEmbeddedBrokerRule.bounce(i);
			}
		}

		latch.await(50, TimeUnit.SECONDS);
		kafkaMessageListenerContainer.stop();

		assertThat(receivedData.valuesView().toList(), hasSize(expectedMessageCount));
		assertThat(latch.getCount(), equalTo(0L));
		System.out.println("All messages received ... checking ");

		validateMessageReceipt(receivedData, 2, 5, 100, expectedMessageCount, readPartitions, 1);

	}

}
