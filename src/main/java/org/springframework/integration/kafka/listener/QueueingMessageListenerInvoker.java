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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;

import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.KafkaMessageMetadata;
import org.springframework.integration.kafka.core.Partition;

/**
 * Invokes a delegate {@link MessageListener} for all the messages passed to it, storing them
 * in an internal queue.
 *
 * @author Marius Bogoevici
 */
class QueueingMessageListenerInvoker implements Runnable {

	private final MessageListener messageListener;

	private final AcknowledgingMessageListener acknowledgingMessageListener;

	private final OffsetManager offsetManager;

	private final ErrorHandler errorHandler;

	private BlockingQueue<KafkaMessage> messages;

	private volatile boolean running = false;

	private volatile CountDownLatch globalShutdownLatch = null;

	private final MutableMap<Partition, CountDownLatch> partitionStopLatches = UnifiedMap.newMap();

	public QueueingMessageListenerInvoker(int capacity, OffsetManager offsetManager, Object delegate,
			ErrorHandler errorHandler) {
		if (delegate instanceof MessageListener) {
			this.messageListener = (MessageListener) delegate;
			this.acknowledgingMessageListener = null;
		}
		else if (delegate instanceof AcknowledgingMessageListener) {
			this.acknowledgingMessageListener = (AcknowledgingMessageListener) delegate;
			this.messageListener = null;
		}
		else {
			// it's neither, an exception will be thrown
			throw new IllegalArgumentException("Either a " + MessageListener.class.getName() + " or a "
					+ AcknowledgingMessageListener.class.getName() + " must be provided");
		}
		this.offsetManager = offsetManager;
		this.errorHandler = errorHandler;
		this.messages = new ArrayBlockingQueue<KafkaMessage>(capacity);
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
				try {
					this.messages.put(message);
					added = true;
				}
				catch (InterruptedException e) {
					// we ignore the interruption signal if we are still running, but pass it on if we are stopped
					wasInterruptedWhileRunning = true;
				}
			}
		}
		if (wasInterruptedWhileRunning) {
			Thread.currentThread().interrupt();
		}
	}

	public void start() {
		this.running = true;
	}

	public void stop(long stopTimeout) {
		globalShutdownLatch = new CountDownLatch(1);
		this.running = false;
		try {
			globalShutdownLatch.await(stopTimeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		messages.clear();
	}

	public void flush(Partition partition, long flushTimeout) {
		if (running) {
			CountDownLatch partitionStopLatch = new CountDownLatch(1);
			partitionStopLatches.put(partition, partitionStopLatch);
			this.enqueue(new KafkaMessage(null, new KafkaMessageMetadata(partition, 0, 0)));
			try {
				partitionStopLatch.await(flushTimeout, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Runs uninterruptibly as long as {@code running} is true, but if interrupted, will defer
	 * propagating the interruption flag at the end.
	 */
	@Override
	public void run() {
		boolean wasInterrupted = false;
		while (this.running) {
			try {
				KafkaMessage message = messages.take();
				if (this.running) {
					// it is not a partition shutdown message
					if (message.getMessage() != null) {
						try {
							if (messageListener != null) {
								messageListener.onMessage(message);
							}
							else {
								acknowledgingMessageListener.onMessage(message, new DefaultAcknowledgment(offsetManager, message));
							}
						}
						catch (Exception e) {
							if (errorHandler != null) {
								errorHandler.handle(e, message);
							}
						}
						finally {
							if (messageListener != null) {
								offsetManager.updateOffset(message.getMetadata().getPartition(),
										message.getMetadata().getNextOffset());
							}
						}
					}
				}
				if (message.getMessage() == null) {
					CountDownLatch latch = partitionStopLatches.get(message.getMetadata().getPartition());
					if (latch != null) {
						latch.countDown();
					}
				}
			}
			catch (InterruptedException e) {
				wasInterrupted = true;
			}
		}
		if (globalShutdownLatch != null) {
			globalShutdownLatch.countDown();
		}
		if (wasInterrupted) {
			Thread.currentThread().interrupt();
		}
	}

}
