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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;

/**
 * Dispatches {@link KafkaMessage}s to a {@link MessageListener}. Messages may be
 * processed concurrently, according to the {@code concurrency} settings, but messages
 * from the same partition are being processed in their original order.
 *
 * When a partition is added, the component will try to allocate it to the thread that is
 * currently processing the smallest number of partitions.
 *
 * Removing partitions may create a load imbalance between threads, as the
 *
 * @author Marius Bogoevici
 */
class ConcurrentMessageListenerDispatcher {

	private static Log log = LogFactory.getLog(ConcurrentMessageListenerDispatcher.class);

	private static final ThreadFactory THREAD_FACTORY = new CustomizableThreadFactory("kafka-listener-dispatcher-");

	private static final Procedure2<QueueingMessageListenerInvoker, Executor> startDelegateProcedure
			= new StartDelegateProcedure();


	private static final Procedure2<QueueingMessageListenerInvoker, Integer> stopDelegateProcedure
			= new StopDelegateProcedure();

	private static final Comparator<Pair<QueueingMessageListenerInvoker, Integer>> invokerLoadComparator
			= new InvokerLoadComparator();

	private final Object lifecycleMonitor = new Object();

	private final Collection<Partition> partitions;

	private final int consumers;

	private final Object delegateListener;

	private final ErrorHandler errorHandler;

	private final OffsetManager offsetManager;

	private final int queueSize;

	private volatile boolean running;

	private final MutableMap<Partition, CountDownLatch> partitionStopLatches = UnifiedMap.newMap();

	// keeps track of the load
	private final MutableMap<Partition, QueueingMessageListenerInvoker> invokersByPartition =
			new ConcurrentHashMap<Partition, QueueingMessageListenerInvoker>();

	private final MutableMap<QueueingMessageListenerInvoker, Integer> loadByInvoker =
			new ConcurrentHashMap<QueueingMessageListenerInvoker, Integer>();

	private Executor taskExecutor;

	public ConcurrentMessageListenerDispatcher(Object delegateListener, ErrorHandler errorHandler,
			Collection<Partition> partitions, OffsetManager offsetManager, int consumers, int queueSize) {
		Assert.isTrue
				(delegateListener instanceof MessageListener
								|| delegateListener instanceof AcknowledgingMessageListener,
						"Either a " + MessageListener.class.getName() + " or a "
								+ AcknowledgingMessageListener.class.getName() + " must be provided");
		Assert.notEmpty(partitions, "A set of partitions must be provided");
//		Assert.isTrue(consumers <= partitions.size(),
//				"The number of consumers must be smaller or equal to the number of partitions");
		Assert.notNull(delegateListener, "A delegate must be provided");
		this.delegateListener = delegateListener;
		this.errorHandler = errorHandler;
		this.partitions = partitions;
		this.offsetManager = offsetManager;
		this.consumers = consumers;
		this.queueSize = queueSize;
	}

	public void start() {
		synchronized (lifecycleMonitor) {
			if (!this.running) {
				initializeAndStartDispatching();
				this.running = true;
			}
		}
	}

	public void stop(int stopTimeout) {
		synchronized (lifecycleMonitor) {
			if (this.running) {
				this.running = false;
				synchronized (invokersByPartition) {
					loadByInvoker.keysView().forEachWith(stopDelegateProcedure, stopTimeout);
				}
			}
		}
	}

	/**
	 *
	 * @param message the message to dispatch
	 *
	 * @return whether the message has been dispatched or not (i.e. ignored)
	 */
	public boolean dispatch(KafkaMessage message) {
		if (this.running) {
			QueueingMessageListenerInvoker queueingMessageListenerInvoker =
					invokersByPartition.get(message.getMetadata().getPartition());
			if (queueingMessageListenerInvoker != null) {
				queueingMessageListenerInvoker.enqueue(message);
				return true;
			}
		}
		return false;
	}

	private void initializeAndStartDispatching() {
		// allocate delegate instances index them
		synchronized (invokersByPartition) {
			List<QueueingMessageListenerInvoker> delegateList = new ArrayList<QueueingMessageListenerInvoker>(consumers);
			for (int i = 0; i < consumers; i++) {
				QueueingMessageListenerInvoker blockingQueueMessageListenerInvoker =
						new QueueingMessageListenerInvoker(queueSize, offsetManager, delegateListener, errorHandler);
				delegateList.add(blockingQueueMessageListenerInvoker);
				loadByInvoker.put(blockingQueueMessageListenerInvoker, 0);
			}
			for (Partition partition : partitions) {
				addPartition(partition);
			}
			// initialize task executor
			if (this.taskExecutor == null) {
				this.taskExecutor = Executors.newFixedThreadPool(consumers, THREAD_FACTORY);
			}
			// start dispatchers
			loadByInvoker.keysView().forEachWith(startDelegateProcedure, taskExecutor);
		}
	}

	public void addPartition(Partition partition) {
		synchronized (invokersByPartition) {
			QueueingMessageListenerInvoker queueingMessageListenerInvoker = invokersByPartition.get(partition);
			if (queueingMessageListenerInvoker == null) {
				Pair<QueueingMessageListenerInvoker, Integer> leastUsedInvoker
						= loadByInvoker.keyValuesView().min(invokerLoadComparator);
				loadByInvoker.put(leastUsedInvoker.getOne(), leastUsedInvoker.getTwo() + 1);
				invokersByPartition.put(partition, leastUsedInvoker.getOne());
			}
		}
	}

	public void removePartition(Partition partition, long stopTimeout) {
		synchronized (invokersByPartition) {
			QueueingMessageListenerInvoker listenedInvoker = invokersByPartition.remove(partition);
			if (listenedInvoker != null) {
				Integer listenedPartitionCount = loadByInvoker.get(listenedInvoker);
				if (listenedPartitionCount == 0) {
					log.warn("Trying to remove the Invoker for " + partition + ", but listened partition count is zero");
				}
				else {
					loadByInvoker.put(listenedInvoker, listenedPartitionCount - 1);
				}
				listenedInvoker.flush(partition, stopTimeout);
			}
		}
	}


	@SuppressWarnings("serial")
	private static class StopDelegateProcedure implements Procedure2<QueueingMessageListenerInvoker, Integer> {

		@Override
		public void value(QueueingMessageListenerInvoker delegate, Integer stopTimeout) {
			delegate.stop(stopTimeout);
		}

	}

	@SuppressWarnings("serial")
	private static class StartDelegateProcedure implements Procedure2<QueueingMessageListenerInvoker, Executor> {

		@Override
		public void value(QueueingMessageListenerInvoker delegate, Executor executor) {
			delegate.start();
			executor.execute(delegate);
		}

	}

	private static class InvokerLoadComparator implements Comparator<Pair<QueueingMessageListenerInvoker, Integer>> {
		@Override
		public int compare(Pair<QueueingMessageListenerInvoker, Integer> o1, Pair<QueueingMessageListenerInvoker, Integer> o2) {
			return o1.getTwo() - o2.getTwo();
		}
	}
}
