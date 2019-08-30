/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.serialization.KakfaWithHeadersDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.serialization.UserDefineVariable;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka consumer API.
 *
 * @param <T> The type of elements produced by the fetcher.
 */
@Internal
public class KafkaFetcher<T> extends AbstractFetcher<T, TopicPartition> {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaFetcher.class);

	// ------------------------------------------------------------------------

	/**
	 * The schema to convert between Kafka's byte messages, and Flink's objects.
	 */
	private final KeyedDeserializationSchema<T> deserializer;

	/**
	 * The handover of data and exceptions between the consumer thread and the task thread.
	 */
	private final Handover handover;

	/**
	 * The thread that runs the actual KafkaConsumer and hand the record batches to this fetcher.
	 */
	private final KafkaConsumerThread consumerThread;

	/**
	 * Flag to mark the main work loop as alive.
	 */
	private volatile boolean running = true;

	private transient long proxyToKafka1Delay;
	private transient long kafka1ToFlinkDelay;
	private transient long proxyToFlinkDelay;
	private transient long flinkToKafka2Delay;
	private transient long totalDelay;
	private transient long proxyRecvTime;
	private transient long kafka1RecvTime;
	private transient long flinkRecvTime;
	private transient long kafka2RecvTime;
	private boolean logEnable = true;
	private int logCount = 0;

	// ------------------------------------------------------------------------

	public KafkaFetcher(
		SourceFunction.SourceContext<T> sourceContext,
		Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
		SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
		SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
		ProcessingTimeService processingTimeProvider,
		long autoWatermarkInterval,
		ClassLoader userCodeClassLoader,
		String taskNameWithSubtasks,
		KeyedDeserializationSchema<T> deserializer,
		Properties kafkaProperties,
		long pollTimeout,
		MetricGroup subtaskMetricGroup,
		MetricGroup consumerMetricGroup,
		boolean useMetrics) throws Exception {
		super(
			sourceContext,
			assignedPartitionsWithInitialOffsets,
			watermarksPeriodic,
			watermarksPunctuated,
			processingTimeProvider,
			autoWatermarkInterval,
			userCodeClassLoader,
			consumerMetricGroup,
			useMetrics);

		this.deserializer = deserializer;
		this.handover = new Handover();

		this.consumerThread = new KafkaConsumerThread(
			LOG,
			handover,
			kafkaProperties,
			unassignedPartitionsQueue,
			getFetcherName() + " for " + taskNameWithSubtasks,
			pollTimeout,
			useMetrics,
			consumerMetricGroup,
			subtaskMetricGroup);

		String topicName = kafkaProperties.getProperty("metric.topic");
		if ("false".equals(kafkaProperties.getProperty("use.default.schema"))) {
			UserDefineVariable.UseDefaultSchema = false;
		} else {
			UserDefineVariable.UseDefaultSchema = true;
		}

		consumerMetricGroup.addGroup("proxyToKafka1Delay").gauge(topicName, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return proxyToKafka1Delay;
			}
		});
		consumerMetricGroup.addGroup("kafka1ToFlinkDelay").gauge(topicName, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return kafka1ToFlinkDelay;
			}
		});
		consumerMetricGroup.addGroup("proxyToFlinkDelay").gauge(topicName, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return proxyToFlinkDelay;
			}
		});
		consumerMetricGroup.addGroup("flinkToKafka2Delay").gauge(topicName, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return flinkToKafka2Delay;
			}
		});
		consumerMetricGroup.addGroup("totalDelay").gauge(topicName, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return totalDelay;
			}
		});
//		consumerMetricGroup.addGroup("proxyRecvTime").gauge(topicName, new Gauge<Long>() {
//			@Override
//			public Long getValue() {
//				return proxyRecvTime;
//			}
//		});
//		consumerMetricGroup.addGroup("kafka1RecvTime").gauge(topicName, new Gauge<Long>() {
//			@Override
//			public Long getValue() {
//				return kafka1RecvTime;
//			}
//		});
//		consumerMetricGroup.addGroup("flinkRecvTime").gauge(topicName, new Gauge<Long>() {
//			@Override
//			public Long getValue() {
//				return flinkRecvTime;
//			}
//		});
//		consumerMetricGroup.addGroup("kafka2RecvTime").gauge(topicName, new Gauge<Long>() {
//			@Override
//			public Long getValue() {
//				return kafka2RecvTime;
//			}
//		});
	}

	// ------------------------------------------------------------------------
	//  Fetcher work methods
	// ------------------------------------------------------------------------

	@Override
	public void runFetchLoop() throws Exception {
		try {
			final Handover handover = this.handover;

			// kick off the actual Kafka consumer
			consumerThread.start();

			while (running) {
				// this blocks until we get the next records
				// it automatically re-throws exceptions encountered in the consumer thread
				final ConsumerRecords<byte[], byte[]> records = handover.pollNext();

				// get the records for each topic partition
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {

					List<ConsumerRecord<byte[], byte[]>> partitionRecords =
						records.records(partition.getKafkaPartitionHandle());

					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
						final T value;
						Header[] headers = record.headers().toArray();
						if (UserDefineVariable.UseDefaultSchema) {
							value = deserializer.deserialize(
								record.key(), record.value(),
								record.topic(), record.partition(), record.offset());
						} else {
							List<String> headerKeys = new ArrayList<>();
							for (Header header : headers) {
								headerKeys.add(header.key());
							}
							if (headerKeys.contains("proxy_recv_time")) {
								// rebuild record, add header information
								record = rebuildRecordHeaders(record, headerKeys.size());
								value = addHeadersIntoValue(record);
							} else {
								value = deserializer.deserialize(
									record.key(), record.value(),
									record.topic(), record.partition(), record.offset());
							}
						}
						if (deserializer.isEndOfStream(value)) {
							// end of stream signaled
							running = false;
							break;
						}
						// emit the actual record. this also updates offset state atomically
						// and deals with timestamps and watermark generation
						emitRecord(value, partition, record.offset(), record);
					}
				}
			}
		} finally {
			// this signals the consumer thread that no more work is to be done
			consumerThread.shutdown();
		}

		// on a clean exit, wait for the runner thread
		try {
			consumerThread.join();
		} catch (InterruptedException e) {
			// may be the result of a wake-up interruption after an exception.
			// we ignore this here and only restore the interruption state
			Thread.currentThread().interrupt();
		}
	}

	protected ConsumerRecord<byte[], byte[]> rebuildRecordHeaders(ConsumerRecord<byte[], byte[]> record, int length) {
		Header[] headers = record.headers().toArray();
		if (length == 1) {
			proxyRecvTime = Long.parseLong(new String(headers[0].value()));
			kafka1RecvTime = record.timestamp();
			flinkRecvTime = System.currentTimeMillis();
			proxyToKafka1Delay = kafka1RecvTime - proxyRecvTime;
			kafka1ToFlinkDelay = flinkRecvTime - kafka1RecvTime;
			proxyToFlinkDelay = flinkRecvTime - proxyRecvTime;

			record.headers().add("kafka1_recv_time", String.valueOf(kafka1RecvTime).getBytes());
			record.headers().add("flink_recv_time", String.valueOf(flinkRecvTime).getBytes());
			record.headers().add("proxy_kafka1_delay", String.valueOf(proxyToKafka1Delay).getBytes());
			record.headers().add("kafka1_flink_delay", String.valueOf(kafka1ToFlinkDelay).getBytes());
			record.headers().add("proxy_flink_delay", String.valueOf(proxyToFlinkDelay).getBytes());
		} else if (length >= 6) {
			long value;
			kafka2RecvTime = record.timestamp();
			for (Header header : headers) {
				value = Long.parseLong(new String(header.value()));
				switch (header.key()) {
					case "proxy_recv_time":
						proxyRecvTime = value;
						break;
					case "kafka1_recv_time":
						kafka1RecvTime = value;
						break;
					case "flink_recv_time":
						flinkRecvTime = value;
						break;
					case "proxy_kafka1_delay":
						proxyToKafka1Delay = value;
						break;
					case "kafka1_flink_delay":
						kafka1ToFlinkDelay = value;
						break;
					case "proxy_flink_delay":
						proxyToFlinkDelay = value;
						break;
					default:
						break;
				}
			}
			flinkToKafka2Delay = kafka2RecvTime - flinkRecvTime;
			totalDelay = kafka2RecvTime - proxyRecvTime;
		}
		return record;
	}

	protected T addHeadersIntoValue(ConsumerRecord<byte[], byte[]> record) {
		T value = null;
		try {
			value = ((KakfaWithHeadersDeserializationSchema<T>) deserializer).deserializeWithHeaders(
				record.headers().toArray(),
				record.key(), record.value(),
				record.topic(), record.partition(), record.offset());
		} catch (Exception e) {
			if (this.logCount < 30) {
				LOG.error("addHeadersIntoValue failed", e);
				this.logCount++;
			}
		}
		if (logEnable) {
			LOG.info("topic:{}, headers:{}", record.topic(), record.headers().toArray());
			LOG.info("proxyRecvTime:{}, kafka1RecvTime:{}, flinkRecvTime:{}, kafka2RecvTime:{}, proxyToKafka1Delay:{}," +
					" kafka1ToFlinkDelay:{}, proxyToFlinkDelay:{}, flinkToKafka2Delay:{}, totalDelay",
				proxyRecvTime, kafka1RecvTime, flinkRecvTime, kafka2RecvTime, proxyToKafka1Delay,
				kafka1ToFlinkDelay, proxyToFlinkDelay, flinkToKafka2Delay, totalDelay);
			logEnable = false;
		}
		return value;
	}

	@Override
	public void cancel() {
		// flag the main thread to exit. A thread interrupt will come anyways.
		running = false;
		handover.close();
		consumerThread.shutdown();
	}

	protected void emitRecord(
		T record,
		KafkaTopicPartitionState<TopicPartition> partition,
		long offset,
		ConsumerRecord<?, ?> consumerRecord) throws Exception {
		emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());
	}

	/**
	 * Gets the name of this fetcher, for thread naming and logging purposes.
	 */
	protected String getFetcherName() {
		return "Kafka Fetcher";
	}

	// ------------------------------------------------------------------------
	//  Implement Methods of the AbstractFetcher
	// ------------------------------------------------------------------------

	@Override
	public TopicPartition createKafkaPartitionHandle(KafkaTopicPartition partition) {
		return new TopicPartition(partition.getTopic(), partition.getPartition());
	}

	@Override
	protected void doCommitInternalOffsetsToKafka(
		Map<KafkaTopicPartition, Long> offsets,
		@Nonnull KafkaCommitCallback commitCallback) throws Exception {

		@SuppressWarnings("unchecked")
		List<KafkaTopicPartitionState<TopicPartition>> partitions = subscribedPartitionStates();

		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(partitions.size());

		for (KafkaTopicPartitionState<TopicPartition> partition : partitions) {
			Long lastProcessedOffset = offsets.get(partition.getKafkaTopicPartition());
			if (lastProcessedOffset != null) {
				checkState(lastProcessedOffset >= 0, "Illegal offset value to commit");

				// committed offsets through the KafkaConsumer need to be 1 more than the last processed offset.
				// This does not affect Flink's checkpoints/saved state.
				long offsetToCommit = lastProcessedOffset + 1;

				offsetsToCommit.put(partition.getKafkaPartitionHandle(), new OffsetAndMetadata(offsetToCommit));
				partition.setCommittedOffset(offsetToCommit);
			}
		}

		// record the work to be committed by the main consumer thread and make sure the consumer notices that
		consumerThread.setOffsetsToCommit(offsetsToCommit, commitCallback);
	}
}
