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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.integration.kafka.core.KafkaMessage;
import reactor.core.processor.RingBufferProcessor;

/**
 * Invokes a delegate {@link MessageListener} for all the messages passed to it, storing them
 * in an internal queue.
 *
 * @author Marius Bogoevici
 */
class QueueingMessageListenerInvoker {

	private final MessageListener messageListener;

	private final AcknowledgingMessageListener acknowledgingMessageListener;

	private final OffsetManager offsetManager;

	private final ErrorHandler errorHandler;

	private RingBufferProcessor<KafkaMessage> disruptor;

	private volatile boolean running = false;

	private volatile CountDownLatch shutdownLatch = null;

	public QueueingMessageListenerInvoker(int capacity, final OffsetManager offsetManager, Object delegate,
	                                      final ErrorHandler errorHandler) {
		if (delegate instanceof MessageListener) {
			this.messageListener = (MessageListener) delegate;
			this.acknowledgingMessageListener = null;
		} else if (delegate instanceof AcknowledgingMessageListener) {
			this.acknowledgingMessageListener = (AcknowledgingMessageListener) delegate;
			this.messageListener = null;
		} else {
			// it's neither, an exception will be thrown
			throw new IllegalArgumentException("Either a " + MessageListener.class.getName() + " or a "
					+ AcknowledgingMessageListener.class.getName() + " must be provided");
		}
		this.offsetManager = offsetManager;
		this.errorHandler = errorHandler;
		this.disruptor = RingBufferProcessor.share("kafka-processor", capacity);
	}

	/**
	 * Add a message to the queue, blocking if the queue has reached its maximum capacity.
	 * Interrupts will be ignored for as long as the component's {@code running} flag is set to true, but will
	 * be deferred for when the method returns.
	 * @param message the KafkaMessage to add
	 */
	public void enqueue(KafkaMessage message) {
		boolean wasInterruptedWhileRunning = false;
		if (this.running) {
			boolean added = false;
			// handle the case when the thread is interrupted while the adapter is still running
			// retry adding the message to the queue until either we succeed, or the adapter is stopped
			while (!added && this.running) {
				disruptor.onNext(message);
				added = true;
			}
		}
		if (wasInterruptedWhileRunning) {
			Thread.currentThread().interrupt();
		}
	}

	public void start() {
		this.running = true;

		disruptor.subscribe(new Subscriber<KafkaMessage>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(KafkaMessage kafkaMessage) {
				try {
					if (messageListener != null) {
						messageListener.onMessage(kafkaMessage);
					} else {
						acknowledgingMessageListener.onMessage(kafkaMessage, new DefaultAcknowledgment(offsetManager, kafkaMessage));
					}
				} catch (Exception e) {
					if (errorHandler != null) {
						errorHandler.handle(e, kafkaMessage);
					}
				} finally {
					if (messageListener != null) {
						offsetManager.updateOffset(kafkaMessage.getMetadata().getPartition(),
								kafkaMessage.getMetadata().getNextOffset());
					}
				}
			}

			@Override
			public void onError(Throwable t) {
				//ignore
			}

			@Override
			public void onComplete() {
				shutdownLatch.countDown();
			}
		});
	}

	public void stop(long stopTimeout) {
		shutdownLatch = new CountDownLatch(1);
		this.running = false;
		try {
			shutdownLatch.await(stopTimeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		disruptor.onComplete();
	}

	/**
	 * Runs uninterruptibly as long as {@code running} is true, but if interrupted, will defer
	 * propagating the interruption flag at the end.
	 */
//	@Override
//	public void run() {
//		boolean wasInterrupted = false;
//		while (this.running) {
//			try {
//				KafkaMessage message = messages.take();
//				if (this.running) {
//					try {
//						if (messageListener != null) {
//							messageListener.onMessage(message);
//						}
//						else {
//							acknowledgingMessageListener.onMessage(message, new DefaultAcknowledgment(offsetManager, message));
//						}
//					}
//					catch (Exception e) {
//						if (errorHandler != null) {
//							errorHandler.handle(e, message);
//						}
//					}
//					finally {
//						if (messageListener != null) {
//							offsetManager.updateOffset(message.getMetadata().getPartition(),
//									message.getMetadata().getNextOffset());
//						}
//					}
//				}
//			}
//			catch (InterruptedException e) {
//				wasInterrupted = true;
//			}
//		}
//		if (shutdownLatch != null) {
//			shutdownLatch.countDown();
//		}
//		if (wasInterrupted) {
//			Thread.currentThread().interrupt();
//		}
//	}


}
