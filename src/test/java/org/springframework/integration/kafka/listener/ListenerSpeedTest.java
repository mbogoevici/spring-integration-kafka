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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.serializer.DefaultDecoder;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.rx.BiStreams;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.KafkaMessageMetadata;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.IdGenerators;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.ReflectionUtils;

/**
 * @author Marius Bogoevici
 */
public class ListenerSpeedTest {

	@Test
	public void testName2() throws Exception {
		ZookeeperConfiguration configuration = new ZookeeperConfiguration("localhost:2181");

		long initialTime = System.currentTimeMillis();
		configuration.setBufferSize(2*1024*1024);

		DefaultConnectionFactory defaultConnectionFactory = new DefaultConnectionFactory(configuration);
		KafkaMessageListenerContainer kafkaMessageListenerContainer =
				new KafkaMessageListenerContainer(defaultConnectionFactory, "test-simple-99");
		final KafkaTopicOffsetManager offsetManager =
				new KafkaTopicOffsetManager(new ZookeeperConnect("localhost:2181"), "topic-offset-manager-listener-"+ System.currentTimeMillis()) {
				};
		offsetManager.setSegmentSize(1024*1024*1024);
//		offsetManager.setBatchWrites(true);
//		offsetManager.setRequiredAcks(0);
//		offsetManager.setMaxBatchSize(1000);
//		offsetManager.setMaxQueueBufferingTime(10000);
		//offsetManager.setMaxBatchSize(10000);
		offsetManager.setRequiredAcks(1);
		offsetManager.setConsumerId(UUID.randomUUID().toString());

		Environment initialize = Environment.initializeIfEmpty();

		final Broadcaster<Tuple2<Partition, Long>> offsets = Broadcaster.create(initialize);
		offsets
				.window(100, 10, TimeUnit.SECONDS).flatMap(new Function<Stream<Tuple2<Partition, Long>>, Publisher<Tuple2<Partition, Long>>>() {
			@Override
			public Publisher<Tuple2<Partition, Long>> apply(Stream<Tuple2<Partition, Long>> tuple2Stream) {
				return BiStreams.reduceByKey(tuple2Stream, new BiFunction<Long, Long, Long>() {
					@Override
					public Long apply(Long aLong, Long aLong2) {
						return Math.max(aLong, aLong2);
					}
				});
			}
		})
				.consume(new Consumer<Tuple2<Partition, Long>>() {
					@Override
					public void accept(Tuple2<Partition, Long> partitionLongTuple2) {
						offsetManager.updateOffset(partitionLongTuple2.getT1(), partitionLongTuple2.getT2());
					}
				});


		kafkaMessageListenerContainer.setOffsetManager(
				new AbstractOffsetManager(defaultConnectionFactory) {
					@Override
					protected void doUpdateOffset(Partition partition, long offset) {
						//offsetManager.updateOffset(partition, offset);
						offsets.onNext(Tuple.of(partition, offset));
					}


					@Override
					protected void doRemoveOffset(Partition partition) {
						offsetManager.doRemoveOffset(partition);
					}

					@Override
					protected Long doGetOffset(Partition partition) {
						return offsetManager.doGetOffset(partition);
					}

					@Override
					public void close() throws IOException {
						offsetManager.close();
					}

					@Override
					public void flush() throws IOException {
						offsetManager.flush();
					}
				});


		offsetManager.afterPropertiesSet();

		DefaultDecoder decoder = new DefaultDecoder(null);

		final AtomicInteger count = new AtomicInteger(0);

		final DefaultMessageBuilderFactory messageBuilderFactory = new DefaultMessageBuilderFactory();

//		kafkaMessageListenerContainer.setMessageListener(new MessageListener() {
//			@Override
//			public void onMessage(KafkaMessage message) {
//				count.incrementAndGet();
//			}
//		});

//		kafkaMessageListenerContainer.setMessageListener(new AbstractDecodingMessageListener<byte[], byte[]>(decoder, decoder) {
//			@Override
//			public void doOnMessage(byte[] key, byte[] payload, KafkaMessageMetadata metadata) {
//				count.incrementAndGet();
//			}
//		});

		Field idGeneratorField = ReflectionUtils.findField(MessageHeaders.class, "idGenerator");
		ReflectionUtils.makeAccessible(idGeneratorField);
		final IdGenerators.SimpleIncrementingIdGenerator idGenerator = new IdGenerators.SimpleIncrementingIdGenerator();
		ReflectionUtils.setField(idGeneratorField, null, idGenerator);

		kafkaMessageListenerContainer.setMessageListener(new AbstractDecodingMessageListener<byte[], byte[]>(decoder, decoder) {
			@Override
			public void doOnMessage(byte[] key, final byte[] payload, KafkaMessageMetadata metadata) {
//				AbstractIntegrationMessageBuilder<byte[]> messageBuilder = messageBuilderFactory
//						.withPayload(payload)
//						.setHeader(KafkaHeaders.MESSAGE_KEY, key)
//						.setHeader(KafkaHeaders.TOPIC, metadata.getPartition().getTopic())
//						.setHeader(KafkaHeaders.PARTITION_ID, metadata.getPartition().getId())
//						.setHeader(KafkaHeaders.OFFSET, metadata.getOffset())
//						.setHeader(KafkaHeaders.NEXT_OFFSET, metadata.getNextOffset());
//				messageBuilder.build();
//				final MessageHeaders headers = new MessageHeaders(Collections.EMPTY_MAP);
				new Message<byte[]>() {
					@Override
					public byte[] getPayload() {
						return payload;
					}

					@Override
					public MessageHeaders getHeaders() {
						//Map<String, Object> headers = new HashMap<String, Object>();
						//headers.put("id", idGenerator.generateId());
						//headers.put("timestamp", System.nanoTime());
						return null;
					}
				};

				//messageBuilderFactory.withPayload(payload).build();

				//new GenericMessage<byte[]>(payload, null);
				MutableMap<String, Object> objects = UnifiedMap.<String, Object>newMap()
						.withKeyValue(KafkaHeaders.MESSAGE_KEY, key)
						.withKeyValue(KafkaHeaders.TOPIC, metadata.getPartition().getTopic())
						.withKeyValue(KafkaHeaders.PARTITION_ID, metadata.getPartition().getId())
						.withKeyValue(KafkaHeaders.OFFSET, metadata.getOffset())
						.withKeyValue(KafkaHeaders.NEXT_OFFSET, metadata.getNextOffset());
//
//				new MessageHeaders(objects);

				new MessageHeaders(null);

				count.incrementAndGet();
			}
		});


		MessageChannel messageChannel = new MessageChannel() {
			@Override
			public boolean send(Message<?> message) {
				count.incrementAndGet();
				return true;
			}

			@Override
			public boolean send(Message<?> message, long timeout) {
				return this.send(message);
			}
		};

		kafkaMessageListenerContainer.setMaxFetch(1024*1024);

		long listenStartTime = System.currentTimeMillis();
		System.out.println("Ready to start after " + (listenStartTime - initialTime)/1000.0);

		kafkaMessageListenerContainer.start();

		while (count.get() < 10000000) {
			System.out.println(count.get() + " " + count.get()/((System.currentTimeMillis() - listenStartTime)/1000.0));
			Thread.sleep(100);
		}

		System.out.println(count + " in " + (System.currentTimeMillis() - listenStartTime)/1000.0);

	}


	@Test
	public void testName3() throws Exception {
		ZookeeperConfiguration configuration = new ZookeeperConfiguration("localhost:2181");

		long initialTime = System.currentTimeMillis();
		configuration.setBufferSize(2*1024*1024);

		DefaultConnectionFactory defaultConnectionFactory = new DefaultConnectionFactory(configuration);
		KafkaMessageListenerContainer kafkaMessageListenerContainer =
				new KafkaMessageListenerContainer(defaultConnectionFactory, "test-simple-99");
		final KafkaTopicOffsetManager offsetManager =
				new KafkaTopicOffsetManager(new ZookeeperConnect("localhost:2181"), "topic-offset-manager-something-"+ System.currentTimeMillis()) {
				};
		offsetManager.setSegmentSize(1024*1024*1024);
//		offsetManager.setBatchWrites(true);
//		offsetManager.setRequiredAcks(0);
//		offsetManager.setMaxBatchSize(1000);
//		offsetManager.setMaxQueueBufferingTime(10000);
		//offsetManager.setMaxBatchSize(10000);
		offsetManager.setRequiredAcks(1);
		offsetManager.setConsumerId(UUID.randomUUID().toString());

		Environment initialize = Environment.initializeIfEmpty();

		final Broadcaster<Tuple2<Partition, Long>> offsets = Broadcaster.create(initialize);
		offsets
				.window(100, 10, TimeUnit.SECONDS).flatMap(new Function<Stream<Tuple2<Partition, Long>>, Publisher<Tuple2<Partition, Long>>>() {
			@Override
			public Publisher<Tuple2<Partition, Long>> apply(Stream<Tuple2<Partition, Long>> tuple2Stream) {
				return BiStreams.reduceByKey(tuple2Stream, new BiFunction<Long, Long, Long>() {
					@Override
					public Long apply(Long aLong, Long aLong2) {
						return Math.max(aLong, aLong2);
					}
				});
			}
		})
				.consume(new Consumer<Tuple2<Partition, Long>>() {
					@Override
					public void accept(Tuple2<Partition, Long> partitionLongTuple2) {
						offsetManager.updateOffset(partitionLongTuple2.getT1(), partitionLongTuple2.getT2());
					}
				});


		kafkaMessageListenerContainer.setOffsetManager(
				new AbstractOffsetManager(defaultConnectionFactory) {
					@Override
					protected void doUpdateOffset(Partition partition, long offset) {
						//offsetManager.updateOffset(partition, offset);
						offsets.onNext(Tuple.of(partition, offset));
					}


					@Override
					protected void doRemoveOffset(Partition partition) {
						offsetManager.doRemoveOffset(partition);
					}

					@Override
					protected Long doGetOffset(Partition partition) {
						return offsetManager.doGetOffset(partition);
					}

					@Override
					public void close() throws IOException {
						offsetManager.close();
					}

					@Override
					public void flush() throws IOException {
						offsetManager.flush();
					}
				});


		offsetManager.afterPropertiesSet();


		final AtomicInteger count = new AtomicInteger(0);

//		kafkaMessageListenerContainer.setMessageListener(new MessageListener() {
//			@Override
//			public void onMessage(KafkaMessage message) {
//				count.incrementAndGet();
//			}
//		});

		kafkaMessageListenerContainer.setMaxFetch(1024*1024);

		long listenStartTime = System.currentTimeMillis();
		System.out.println("Ready to start after " + (listenStartTime - initialTime)/1000.0);

		//kafkaMessageListenerContainer.start();

		KafkaMessageDrivenChannelAdapter channelAdapter = new KafkaMessageDrivenChannelAdapter(kafkaMessageListenerContainer);

		DefaultDecoder decoder = new DefaultDecoder(null);
		channelAdapter.setKeyDecoder(decoder);
		channelAdapter.setPayloadDecoder(decoder);

		channelAdapter.setOutputChannel(new MessageChannel() {
			@Override
			public boolean send(Message<?> message) {
				count.incrementAndGet();
				return true;
			}

			@Override
			public boolean send(Message<?> message, long timeout) {
				return this.send(message);
			}
		});

		channelAdapter.afterPropertiesSet();
		channelAdapter.start();


		while (count.get() < 10000000) {
			System.out.println(count.get() + " " + count.get()/((System.currentTimeMillis() - listenStartTime)/1000.0));
			Thread.sleep(100);
		}


		System.out.println(count + " in " + (System.currentTimeMillis() - listenStartTime)/1000.0);

	}

	@Test
	public void testName() throws Exception {

		SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", 9092, 10000, 2000000, "klient");

		OffsetResponse klient = simpleConsumer.getOffsetsBefore(new OffsetRequest(Collections.singletonMap(new TopicAndPartition("test-simple", 0), new PartitionOffsetRequestInfo(-2, 1)), kafka.api.OffsetRequest.CurrentVersion(), "klient"));

		long offset = klient.offsets("test-simple",0)[0];
		int count = 0;
		long initialTime = System.currentTimeMillis();
		do {
			FetchResponse fetch = simpleConsumer.fetch(new FetchRequestBuilder().addFetch("test-simple", 0, offset, 1000000).build());
			ByteBufferMessageSet messageAndOffsets = fetch.messageSet("test-simple", 0);
			for (MessageAndOffset messageAndOffset : messageAndOffsets) {
				offset = messageAndOffset.nextOffset();
				messageAndOffset.message().payload();
				messageAndOffset.message().key();
				count ++;
			}
			if (offset == fetch.highWatermark("test-simple", 0))
				break;
		} while(true);
		System.out.println(count + " in " + (System.currentTimeMillis() - initialTime)/1000.0);
	}
}
