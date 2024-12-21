package kafka.protocol.serdes;

import kafka.protocol.Message;
import kafka.protocol.RequestApi;
import kafka.protocol.io.DataInput;
import kafka.util.function.UnsafeFunction;
import lombok.SneakyThrows;

public interface MessageDeserializer<T extends Message> {

	T deserialize(DataInput input);

	RequestApi requestApi();

	public static <T extends Message> MessageDeserializer<T> of(RequestApi requestApi, UnsafeFunction<DataInput, T> deserializer) {
		return new MessageDeserializer<T>() {

			@SneakyThrows
			@Override
			public T deserialize(DataInput input) {
				return deserializer.apply(input);
			}

			@Override
			public RequestApi requestApi() {
				return requestApi;
			}

		};
	}

}