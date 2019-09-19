package org.apache.flink.streaming.connectors.kafka.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * KafkaWithHeadersSerializationSchema.
 */
public class KafkaWithHeadersSerializationSchema<T> implements KeyedSerializationSchema<T> {

	private final SerializationSchema<T> serializationSchema;

	public KafkaWithHeadersSerializationSchema(SerializationSchema<T> serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	@Override
	public byte[] serializeKey(T element) {
		return null;
	}

	@Override
	public byte[] serializeValue(T element) {
		return serializationSchema.serialize(element);
	}

	@Override
	public String getTargetTopic(T element) {
		return null;
	}

}
