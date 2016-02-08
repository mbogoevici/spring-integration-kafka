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

package org.springframework.integration.kafka.performance;

import static org.hamcrest.CoreMatchers.is;
import static org.springframework.integration.kafka.util.TopicUtils.ensureTopicCreated;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;

import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.listener.KafkaNativeOffsetManager;
import org.springframework.integration.kafka.listener.KafkaTopicOffsetManager;
import org.springframework.integration.kafka.rule.KafkaEmbedded;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.util.StopWatch;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function2;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.factory.Multimaps;
import com.gs.collections.impl.tuple.Tuples;

/**
 * @author Marius Bogoevici
 */
public class OffsetManagerPerformanceTests {

	@Rule
	public KafkaEmbedded embedded = new KafkaEmbedded(1);

	@Test
	public void topicManagerPerformanceTests() throws Exception {
		KafkaTopicOffsetManager kafkaTopicOffsetManager = new KafkaTopicOffsetManager(new ZookeeperConnect(embedded.getZookeeperConnectionString()), "zkTopic");
		kafkaTopicOffsetManager.afterPropertiesSet();
		createTopic(embedded.getZkClient(), "sometopic", 1, 1, 1);
		Partition partition = new Partition("sometopic",0);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (long i=0; i<100000; i++) {
			kafkaTopicOffsetManager.updateOffset(partition, i);
		}
		stopWatch.stop();
		kafkaTopicOffsetManager.close();
		System.out.println("KafkaTopicOffsetManager completed in " +  stopWatch.getTotalTimeSeconds() + "s");
		kafkaTopicOffsetManager = new KafkaTopicOffsetManager(new ZookeeperConnect(embedded.getZookeeperConnectionString()), "zkTopic");
		kafkaTopicOffsetManager.afterPropertiesSet();
		Assert.assertThat(kafkaTopicOffsetManager.getOffset(partition), is(99999L));
	}

	@Test
	public void nativeManagerPerformanceTests() throws Exception {

		String zookeeperConnectionString = embedded.getZookeeperConnectionString();
		ZookeeperConnect zookeeperConnect = new ZookeeperConnect(zookeeperConnectionString);
		KafkaNativeOffsetManager kafkaNativeOffsetManager = new KafkaNativeOffsetManager(
				new DefaultConnectionFactory(new ZookeeperConfiguration(zookeeperConnect)), zookeeperConnect);
		kafkaNativeOffsetManager.afterPropertiesSet();
		Partition partition = new Partition("sometopic", 0);
		createTopic(embedded.getZkClient(), "sometopic", 1, 1, 1);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (long i=0; i<100000; i++) {
			kafkaNativeOffsetManager.updateOffset(partition, i);
		}
		stopWatch.stop();
		System.out.println("KafkaNativeOffsetManager completed in " +  stopWatch.getTotalTimeSeconds() + "s");
		kafkaNativeOffsetManager = new KafkaNativeOffsetManager(
				new DefaultConnectionFactory(new ZookeeperConfiguration(zookeeperConnect)), zookeeperConnect);
		kafkaNativeOffsetManager.afterPropertiesSet();
		Assert.assertThat(kafkaNativeOffsetManager.getOffset(partition), is(99999L));

	}

	@SuppressWarnings("unchecked")
	public void createTopic(ZkClient zkClient, String topicName, int partitionCount, int brokers, int replication) {
		MutableMultimap<Integer, Integer> partitionDistribution =
				createPartitionDistribution(partitionCount, brokers, replication);
		ensureTopicCreated(zkClient, topicName, partitionCount, new Properties(),
				toKafkaPartitionMap(partitionDistribution));
	}


	private KafkaEmbedded getKafkaRule() {
		return embedded;
	}

	public MutableMultimap<Integer, Integer> createPartitionDistribution(int partitionCount, int brokers,
																		 int replication) {
		MutableMultimap<Integer, Integer> partitionDistribution = Multimaps.mutable.list.with();
		for (int i = 0; i < partitionCount; i++) {
			for (int j = 0; j < replication; j++) {
				partitionDistribution.put(i, (i + j) % brokers);
			}
		}
		return partitionDistribution;
	}

	@SuppressWarnings({"rawtypes", "serial", "deprecation"})
	private Map toKafkaPartitionMap(Multimap<Integer, Integer> partitions) {
		java.util.Map<Object, Seq<Object>> m = partitions.toMap()
				.collect(new Function2<Integer, RichIterable<Integer>, Pair<Object, Seq<Object>>>() {

					@Override
					public Pair<Object, Seq<Object>> value(Integer argument1, RichIterable<Integer> argument2) {
						return Tuples.pair((Object) argument1,
								List$.MODULE$.fromArray(argument2.toArray(new Object[0])).toSeq());
					}

				});
		return Map$.MODULE$.apply(JavaConversions.asScalaMap(m).toSeq());
	}

}
