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

package org.springframework.integration.kafka.listener;

import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import kafka.message.Message;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.KafkaMessageMetadata;
import org.springframework.integration.kafka.core.Partition;

/**
 * @author Marius Bogoevici
 */
public class ConcurrentMessageListenerDispatcherTests {

	public static final Message MOCK_KAFKA_MESSAGE = Mockito.mock(Message.class);

	@Test
	public void testMessagesDispatched() throws Exception {

		OffsetManager offsetManager = Mockito.mock(OffsetManager.class);
		final CountDownLatch latch = new CountDownLatch(1);
		MessageListener messageListener = new MessageListener() {
			@Override
			public void onMessage(KafkaMessage message) {
				latch.countDown();
			}
		};
		ErrorHandler errorHandler = new ErrorHandler() {
			@Override
			public void handle(Exception thrownException, KafkaMessage message) {

			}
		};
		QueueingMessageListenerInvoker invoker =
				new QueueingMessageListenerInvoker(10, offsetManager, messageListener, errorHandler);

		invoker.start();

		Executors.newSingleThreadExecutor().execute(invoker);

		invoker.enqueue(new KafkaMessage(MOCK_KAFKA_MESSAGE, new KafkaMessageMetadata(new Partition("t1", 0), 0, 1)));

		Assert.assertThat(latch.await(1000, TimeUnit.SECONDS), is(true));

		invoker.stop(100);
	}

	@Test
	public void testDispatchHappensOnlyToListenedPartitions() throws Exception {

		final CountDownLatch latch = new CountDownLatch(2);
		MessageListener messageListener = new MessageListener() {
			@Override
			public void onMessage(KafkaMessage message) {
				latch.countDown();
			}
		};
		ErrorHandler errorHandler = new ErrorHandler() {
			@Override
			public void handle(Exception thrownException, KafkaMessage message) {

			}
		};
		OffsetManager offsetManager = Mockito.mock(OffsetManager.class);
		ConcurrentMessageListenerDispatcher dispatcher = new ConcurrentMessageListenerDispatcher(
				messageListener, errorHandler, Collections.singleton(new Partition("t1",0)),offsetManager,2,5);

		dispatcher.start();

		Assert.assertThat(dispatcher.dispatch(new KafkaMessage(MOCK_KAFKA_MESSAGE,
				new KafkaMessageMetadata(new Partition("t1", 0), 0, 1))), is(true));

		// a message is not dispatched to partition that the component is not listening to
		Assert.assertThat(dispatcher.dispatch(new KafkaMessage(MOCK_KAFKA_MESSAGE,
				new KafkaMessageMetadata(new Partition("t2", 0), 0, 1))), is(false));

		dispatcher.addPartition(new Partition("t2", 0));

		Assert.assertThat(dispatcher.dispatch(new KafkaMessage(MOCK_KAFKA_MESSAGE,
				new KafkaMessageMetadata(new Partition("t2", 0), 0, 1))), is(true));

		Assert.assertThat(latch.await(1, TimeUnit.SECONDS), is(true));

		dispatcher.stop(100);

	}

	@Test
	public void testRemoveStopsDispatch() throws Exception {

		final CountDownLatch latch = new CountDownLatch(2);
 		MessageListener messageListener = new MessageListener() {
			@Override
			public void onMessage(KafkaMessage message) {
				latch.countDown();
			}
		};
		ErrorHandler errorHandler = new ErrorHandler() {
			@Override
			public void handle(Exception thrownException, KafkaMessage message) {

			}
		};
		OffsetManager offsetManager = Mockito.mock(OffsetManager.class);
		ConcurrentMessageListenerDispatcher dispatcher = new ConcurrentMessageListenerDispatcher(
				messageListener, errorHandler,
				UnifiedSet.newSetWith(new Partition("t1", 0), new Partition("t2",0)),offsetManager,1,5);

		dispatcher.start();

		Assert.assertThat(dispatcher.dispatch(new KafkaMessage(MOCK_KAFKA_MESSAGE,
				new KafkaMessageMetadata(new Partition("t1", 0), 0, 1))), is(true));

		Assert.assertThat(dispatcher.dispatch(new KafkaMessage(MOCK_KAFKA_MESSAGE,
				new KafkaMessageMetadata(new Partition("t2", 0), 0, 1))), is(true));

		dispatcher.removePartition(new Partition("t2", 0), 1000);

		// a message is not dispatched to partition that the component is not listening to
		Assert.assertThat(dispatcher.dispatch(new KafkaMessage(MOCK_KAFKA_MESSAGE,
				new KafkaMessageMetadata(new Partition("t2", 0), 0, 1))), is(false));

		Assert.assertThat(latch.await(1, TimeUnit.SECONDS), is(true));

		dispatcher.stop(100);

	}
}
