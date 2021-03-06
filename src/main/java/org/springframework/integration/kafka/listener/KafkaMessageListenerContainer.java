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

import static com.gs.collections.impl.utility.ArrayIterate.flatCollect;
import static com.gs.collections.impl.utility.Iterate.partition;
import static com.gs.collections.impl.utility.MapIterate.forEachKeyValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.ConsumerException;
import org.springframework.integration.kafka.core.FetchRequest;
import org.springframework.integration.kafka.core.KafkaConsumerDefaults;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.KafkaMessageBatch;
import org.springframework.integration.kafka.core.KafkaTemplate;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.Result;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.collection.MutableCollection;
import com.gs.collections.api.list.ImmutableList;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.api.partition.PartitionIterable;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.block.function.checked.CheckedFunction;
import com.gs.collections.impl.factory.Lists;
import com.gs.collections.impl.factory.Multimaps;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.Iterate;

import kafka.common.ErrorMapping;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageListenerContainer implements SmartLifecycle {

	public static final int DEFAULT_WAIT_FOR_LEADER_REFRESH_RETRY = 5000;

	private static final int DEFAULT_STOP_TIMEOUT = 1000;

	private static final Log log = LogFactory.getLog(KafkaMessageListenerContainer.class);

	public static final Function<Map.Entry<Partition, ?>, Partition> keyFunction = Functions.getKeyFunction();

	private final GetOffsetForPartitionFunction getOffset = new GetOffsetForPartitionFunction();

	private final PartitionToLeaderFunction getLeader = new PartitionToLeaderFunction();

	private final Function<Partition, Partition> passThru = Functions.getPassThru();

	private final LaunchFetchTaskProcedure launchFetchTask = new LaunchFetchTaskProcedure();

	private final Object lifecycleMonitor = new Object();

	private final KafkaTemplate kafkaTemplate;

	private final String[] topics;

	private Partition[] partitions;

	public boolean autoStartup = true;

	private Executor fetchTaskExecutor;

	private Executor adminTaskExecutor = Executors.newSingleThreadExecutor();

	private Executor dispatcherTaskExecutor;

	private int concurrency = 1;

	private volatile boolean running = false;

	private int maxFetch = KafkaConsumerDefaults.FETCH_SIZE_INT;

	private int queueSize = 1024;

	private int stopTimeout = DEFAULT_STOP_TIMEOUT;

	private Object messageListener;

	private ErrorHandler errorHandler = new LoggingErrorHandler();

	private volatile OffsetManager offsetManager;

	private ConcurrentMap<Partition, Long> fetchOffsets;

	private ConcurrentMessageListenerDispatcher messageDispatcher;

	private final MutableMultimap<BrokerAddress, Partition> partitionsByBrokerMap = Multimaps.mutable.set.with();

	private boolean autoCommitOnError = false;

	public KafkaMessageListenerContainer(ConnectionFactory connectionFactory, Partition... partitions) {
		Assert.notNull(connectionFactory, "A connection factory must be supplied");
		Assert.notEmpty(partitions, "A list of partitions must be provided");
		Assert.noNullElements(partitions, "The list of partitions cannot contain null elements");
		this.kafkaTemplate = new KafkaTemplate(connectionFactory);
		this.partitions = partitions;
		this.topics = null;
	}

	public KafkaMessageListenerContainer(final ConnectionFactory connectionFactory, String... topics) {
		Assert.notNull(connectionFactory, "A connection factory must be supplied");
		Assert.notNull(topics, "A list of topics must be provided");
		Assert.noNullElements(topics, "The list of topics cannot contain null elements");
		this.kafkaTemplate = new KafkaTemplate(connectionFactory);
		this.topics = topics;
	}

	public OffsetManager getOffsetManager() {
		return offsetManager;
	}

	public void setOffsetManager(OffsetManager offsetManager) {
		this.offsetManager = offsetManager;
	}

	public Object getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(Object messageListener) {
		Assert.isTrue
				(messageListener instanceof MessageListener
								|| messageListener instanceof AcknowledgingMessageListener,
						"Either a " + MessageListener.class.getName() + " or a "
								+ AcknowledgingMessageListener.class.getName() + " must be provided");
		this.messageListener = messageListener;
	}

	public ErrorHandler getErrorHandler() {
		return errorHandler;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	public int getConcurrency() {
		return concurrency;
	}

	/**
	 * The maximum number of concurrent {@link MessageListener}s running. Messages from within the same
	 * partition will be processed sequentially.
	 * @param concurrency the concurrency maximum number
	 */
	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * The timeout for waiting for each concurrent {@link MessageListener} to finish on stopping.
	 * @param stopTimeout timeout in milliseconds
	 * @since 1.1
	 */
	public void setStopTimeout(int stopTimeout) {
		this.stopTimeout = stopTimeout;
	}

	public int getStopTimeout() {
		return stopTimeout;
	}

	public Executor getFetchTaskExecutor() {
		return fetchTaskExecutor;
	}

	/**
	 * The task executor for fetch operations
	 * @param fetchTaskExecutor the Executor for fetch operations
	 */
	public void setFetchTaskExecutor(Executor fetchTaskExecutor) {
		this.fetchTaskExecutor = fetchTaskExecutor;
	}


	public Executor getAdminTaskExecutor() {
		return adminTaskExecutor;
	}

	/**
	 * The task executor for leader and offset updates
	 * @param adminTaskExecutor the task executor for leader and offset updates.
	 */
	public void setAdminTaskExecutor(Executor adminTaskExecutor) {
		this.adminTaskExecutor = adminTaskExecutor;
	}

	/**
	 * The task executor for invoking the MessageListener
	 * @param dispatcherTaskExecutor the task executor for invoking the MessageListener
	 */
	public void setDispatcherTaskExecutor(Executor dispatcherTaskExecutor) {
		this.dispatcherTaskExecutor = dispatcherTaskExecutor;
	}

	/**
	 * @return the maximum amount of data (in bytes) that pollers will fetch in one round
	 */
	public int getMaxFetch() {
		return maxFetch;
	}

	public int getQueueSize() {
		return queueSize;
	}

	/**
	 * The maximum number of messages that are buffered by each concurrent {@link MessageListener} runner.
	 * Increasing the value may increase throughput, but also increases the memory consumption.
	 * Must be a positive number and a power of 2.
	 * @param queueSize the queue size
	 */
	public void setQueueSize(int queueSize) {
		Assert.isTrue(queueSize > 0 && Integer.bitCount(queueSize) == 1,
				"'queueSize' must be a positive number and a power of 2");
		this.queueSize = queueSize;
	}

	public void setMaxFetch(int maxFetch) {
		this.maxFetch = maxFetch;
	}

	/**
	 * Whether offsets should be auto acknowledged even when exceptions are thrown during processing. This setting
	 * is effective only in auto acknowledged mode. When set to true, all received messages will be acknowledged,
	 * and when set to false only the offset of the last successfully processed message is persisted, even if the
	 * component will try to continue processing incoming messages. In the latter case, it is possible that
	 * a successful message will commit an offset after a series of failures, so the component should rely on
	 * the `errorHandler` to capture failures.
	 *
	 * @param autoCommitOnError false if offsets should be committed only for successful messages
	 * @since 1.3
	 */
	public void setAutoCommitOnError(boolean autoCommitOnError) {
		this.autoCommitOnError = autoCommitOnError;
	}

	public boolean isAutoCommitOnError() {
		return autoCommitOnError;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (lifecycleMonitor) {
			if (running) {
				this.running = false;
				try {
					this.offsetManager.flush();
				}
				catch (IOException e) {
					log.error("Error while flushing:", e);
				}
				this.messageDispatcher.stop(stopTimeout);
			}
		}
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public void start() {
		synchronized (lifecycleMonitor) {
			if (!running) {
				if (partitions == null) {
					partitions = getPartitionsForTopics(kafkaTemplate.getConnectionFactory(), topics);
				}
				this.running = true;
				if (this.offsetManager == null) {
					this.offsetManager = new MetadataStoreOffsetManager(kafkaTemplate.getConnectionFactory());
				}
				// initialize the fetch offset table - defer to OffsetManager for retrieving them
				ImmutableList<Partition> partitionsAsList = Lists.immutable.with(partitions);
				this.fetchOffsets = new ConcurrentHashMap<Partition, Long>(partitionsAsList.toMap(passThru, getOffset));
				this.messageDispatcher = new ConcurrentMessageListenerDispatcher(messageListener, errorHandler,
						Arrays.asList(partitions), offsetManager, concurrency, queueSize, dispatcherTaskExecutor,
						autoCommitOnError);
				this.messageDispatcher.start();
				partitionsByBrokerMap.clear();
				partitionsByBrokerMap.putAll(partitionsAsList.groupBy(getLeader));
				if (fetchTaskExecutor == null) {
					fetchTaskExecutor = Executors.newFixedThreadPool(partitionsByBrokerMap.keysView().size());
				}
				partitionsByBrokerMap.forEachKey(launchFetchTask);
			}
		}
	}

	@Override
	public void stop() {
		this.stop(null);
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	private static Partition[] getPartitionsForTopics(final ConnectionFactory connectionFactory, String[] topics) {
		MutableList<Partition> partitionList = flatCollect(topics, new GetPartitionsForTopic(connectionFactory));
		return partitionList.toArray(new Partition[partitionList.size()]);
	}

	/**
	 * Fetches data from Kafka for a group of partitions, located on the same broker.
	 */
	public class FetchTask implements SchedulingAwareRunnable {

		private final BrokerAddress brokerAddress;

		public FetchTask(BrokerAddress brokerAddress) {
			this.brokerAddress = brokerAddress;
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			boolean wasInterrupted = false;
			while (isRunning()) {
				MutableCollection<Partition> fetchPartitions;
				synchronized (partitionsByBrokerMap) {
					// retrieve the partitions for the current polling cycle
					fetchPartitions = partitionsByBrokerMap.get(brokerAddress);
					// do not proceed until there is something to read from
					while (isRunning() && CollectionUtils.isEmpty(fetchPartitions)) {
						try {
							// we only got here because there were no partitions to read from,
							// so block until there is a change this prevents FetchTasks
							// from busy waiting while leaders or offsets are being refreshed
							// TODO: ideally we should use separate monitors for each task
							partitionsByBrokerMap.wait();
							// see if the changes affect us
							fetchPartitions = partitionsByBrokerMap.get(brokerAddress);
						}
						catch (InterruptedException e) {
							wasInterrupted = true;
						}
					}
				}
				// we've just exited a potentially blocking operation. Is the component still running?
				if (isRunning()) {
					Set<Partition> partitionsWithRemainingData;
					boolean hasErrors;
					do {
						partitionsWithRemainingData = new HashSet<Partition>();
						hasErrors = false;
						try {
							MutableCollection<FetchRequest> fetchRequests =
									fetchPartitions.collect(new PartitionToFetchRequestFunction());
							Result<KafkaMessageBatch> result = kafkaTemplate.receive(fetchRequests);
							// process successful messages first
							Iterable<KafkaMessageBatch> batches = result.getResults().values();
							for (KafkaMessageBatch batch : batches) {
								if (!batch.getMessages().isEmpty()) {
									long highestFetchedOffset = 0;
									for (KafkaMessage kafkaMessage : batch.getMessages()) {
										// fetch operations may return entire blocks of compressed messages,
										// which may have lower offsets than the ones requested
										// thus a batch may contain messages that have been processed already
										if (kafkaMessage.getMetadata().getOffset() >= fetchOffsets.get(batch.getPartition())) {
											messageDispatcher.dispatch(kafkaMessage);
										}
										highestFetchedOffset =
												Math.max(highestFetchedOffset, kafkaMessage.getMetadata().getNextOffset());
									}
									fetchOffsets.replace(batch.getPartition(), highestFetchedOffset);
									// if there are still messages on server, we can go on and retrieve more
									if (highestFetchedOffset < batch.getHighWatermark()) {
										partitionsWithRemainingData.add(batch.getPartition());
									}
								}
							}
							// handle errors
							if (result.getErrors().size() > 0) {
								hasErrors = true;

								// find partitions with leader errors and
								PartitionIterable<Map.Entry<Partition, Short>> partitionByLeaderErrors =
										partition(result.getErrors().entrySet(), new IsLeaderErrorPredicate());
								RichIterable<Partition> partitionsWithLeaderErrors =
										partitionByLeaderErrors.getSelected().collect(keyFunction);
								resetLeaders(partitionsWithLeaderErrors);

								PartitionIterable<Map.Entry<Partition, Short>> partitionsWithOffsetsOutOfRange =
										partitionByLeaderErrors.getRejected()
												.partition(new IsOffsetOutOfRangePredicate());
								resetOffsets(partitionsWithOffsetsOutOfRange.getSelected()
										.collect(keyFunction)
										.toSet());
								// it's not a leader issue
								stopFetchingFromPartitions(partitionsWithOffsetsOutOfRange.getRejected()
										.collect(keyFunction));
							}
						}
						catch (ConsumerException e) {
							resetLeaders(fetchPartitions.toImmutable());
						}
					} while (!hasErrors && isRunning() && !partitionsWithRemainingData.isEmpty());
				}
			}
			if (wasInterrupted) {
				Thread.currentThread().interrupt();
			}
		}


		private void resetLeaders(final Iterable<Partition> partitionsToReset) {
			stopFetchingFromPartitions(partitionsToReset);
			adminTaskExecutor.execute(new UpdateLeadersTask(partitionsToReset));
		}


		private void resetOffsets(final Collection<Partition> partitionsToResetOffsets) {
			stopFetchingFromPartitions(partitionsToResetOffsets);
			adminTaskExecutor.execute(new UpdateOffsetsTask(partitionsToResetOffsets));
		}

		private void stopFetchingFromPartitions(Iterable<Partition> partitions) {
			synchronized (partitionsByBrokerMap) {
				for (Partition partition : partitions) {
					partitionsByBrokerMap.remove(brokerAddress, partition);
				}
			}
		}

		private class UpdateLeadersTask implements SchedulingAwareRunnable {
			private final Iterable<Partition> partitionsToReset;

			public UpdateLeadersTask(Iterable<Partition> partitionsToReset) {
				this.partitionsToReset = partitionsToReset;
			}

			@Override
			public boolean isLongLived() {
				return true;
			}

			@Override
			public void run() {
				// fetch can complete successfully or unsuccessfully
				boolean fetchCompleted = false;
				while (!fetchCompleted && isRunning()) {
					try {
						FastList<Partition> partitionsAsList = FastList.newList(partitionsToReset);
						FastList<String> topics = partitionsAsList.collect(new PartitionToTopicFunction()).distinct();
						kafkaTemplate.getConnectionFactory().refreshMetadata(topics);
						Map<Partition, BrokerAddress> leaders = kafkaTemplate.getConnectionFactory().getLeaders(partitionsToReset);
						synchronized (partitionsByBrokerMap) {
							forEachKeyValue(leaders, new AddPartitionToBrokerProcedure());
							partitionsByBrokerMap.notifyAll();
						}
						fetchCompleted = true;
					}
					catch (Exception e) {
						if (isRunning()) {
							try {
								Thread.sleep(DEFAULT_WAIT_FOR_LEADER_REFRESH_RETRY);
							}
							catch (InterruptedException e1) {
								Thread.currentThread().interrupt();
								log.error("Interrupted after refresh leaders failure for: " + Iterate.makeString(partitionsToReset,","));
								fetchCompleted = true;
							}
						}
					}
				}
			}

		}

		private class UpdateOffsetsTask implements Runnable {

			private final Collection<Partition> partitionsToResetOffsets;

			public UpdateOffsetsTask(Collection<Partition> partitionsToResetOffsets) {
				this.partitionsToResetOffsets = partitionsToResetOffsets;
			}

			@Override
			public void run() {
				offsetManager.resetOffsets(partitionsToResetOffsets);
				for (Partition partition : partitionsToResetOffsets) {
					fetchOffsets.replace(partition, offsetManager.getOffset(partition));
				}
				synchronized (partitionsByBrokerMap) {
					for (Partition partitionsToResetOffset : partitionsToResetOffsets) {
						partitionsByBrokerMap.put(brokerAddress, partitionsToResetOffset);
					}
					// notify any waiting task that the partition allocation has changed
					partitionsByBrokerMap.notifyAll();
				}
			}

		}

		@SuppressWarnings("serial")
		private class IsLeaderErrorPredicate implements Predicate<Map.Entry<Partition, Short>> {

			@Override
			public boolean accept(Map.Entry<Partition, Short> each) {
				return each.getValue() == ErrorMapping.NotLeaderForPartitionCode()
						|| each.getValue() == ErrorMapping.UnknownTopicOrPartitionCode();
			}

		}

		@SuppressWarnings("serial")
		private class IsOffsetOutOfRangePredicate implements Predicate<Map.Entry<Partition, Short>> {

			@Override
			public boolean accept(Map.Entry<Partition, Short> each) {
				return each.getValue() == ErrorMapping.OffsetOutOfRangeCode();
			}

		}
	}

	@SuppressWarnings("serial")
	class GetOffsetForPartitionFunction extends CheckedFunction<Partition, Long> {

		@Override
		public Long safeValueOf(Partition object) throws Exception {
			try {
				return offsetManager.getOffset(object);
			}
			catch (Exception e) {
				log.error(e);
				throw e;
			}
		}

	}

	@SuppressWarnings("serial")
	private class PartitionToLeaderFunction implements Function<Partition, BrokerAddress> {

		@Override
		public BrokerAddress valueOf(Partition partition) {
			return kafkaTemplate.getConnectionFactory().getLeader(partition);
		}

	}

	@SuppressWarnings("serial")
	private class LaunchFetchTaskProcedure implements Procedure<BrokerAddress> {

		@Override
		public void value(BrokerAddress brokerAddress) {
			fetchTaskExecutor.execute(new FetchTask(brokerAddress));
		}

	}

	@SuppressWarnings("serial")
	private class PartitionToFetchRequestFunction implements Function<Partition, FetchRequest> {

		@Override
		public FetchRequest valueOf(Partition partition) {
			return new FetchRequest(partition, fetchOffsets.get(partition), maxFetch);
		}

	}

	@SuppressWarnings("serial")
	static class GetPartitionsForTopic extends CheckedFunction<String, Iterable<Partition>> {

		private final ConnectionFactory connectionFactory;

		public GetPartitionsForTopic(ConnectionFactory connectionFactory) {
			this.connectionFactory = connectionFactory;
		}

		@Override
		public Iterable<Partition> safeValueOf(String topic) throws Exception {
			return connectionFactory.getPartitions(topic);
		}

	}

	@SuppressWarnings("serial")
	private class PartitionToTopicFunction implements Function<Partition, String> {

		@Override
		public String valueOf(Partition object) {
			return object.getTopic();
		}

	}

	@SuppressWarnings("serial")
	private class AddPartitionToBrokerProcedure implements Procedure2<Partition, BrokerAddress> {

		@Override
		public void value(Partition partition, BrokerAddress newBrokerAddress) {
			partitionsByBrokerMap.put(newBrokerAddress, partition);
		}

	}

}
