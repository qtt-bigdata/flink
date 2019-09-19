package org.apache.flink.streaming.connectors.kafka.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * KakfaWithHeadersDeserializationSchema.
 */
public class KakfaWithHeadersDeserializationSchema<T> implements KafkaDeserializationSchema<T> {

	private final DeserializationSchema<T> deserializationSchema;

	public KakfaWithHeadersDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		return deserializationSchema.deserialize(record.value());
	}

	public T deserializeWithHeaders(ConsumerRecord<byte[], byte[]> record) {
		Header[] headers = record.headers().toArray();
		byte[] message = record.value();

		Map<String, byte[]> map = new HashMap<>();
		for (Header header : headers) {
			map.put(header.key(), header.value());
		}
		map.put("message", message);

		try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			 ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
			out.writeObject(map);
			return (T) byteOut.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return deserializationSchema.isEndOfStream(nextElement);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
