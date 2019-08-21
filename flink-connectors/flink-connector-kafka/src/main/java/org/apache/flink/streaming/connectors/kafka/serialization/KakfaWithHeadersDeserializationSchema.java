package org.apache.flink.streaming.connectors.kafka.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.kafka.common.header.Header;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * KakfaWithHeadersDeserializationSchema.
 */
public class KakfaWithHeadersDeserializationSchema<T> implements KeyedDeserializationSchema<T> {

	private final DeserializationSchema<T> deserializationSchema;

	public KakfaWithHeadersDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public T deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
		return deserializationSchema.deserialize(message);
	}

	public T deserializeWithHeaders(Header[] headers, byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
		Map<String, byte[]> map = new HashMap<>();
		for (Header header : headers) {
			map.put(header.key(), header.value());
		}
		map.put("message", message);

		try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
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
