package kafka.protocol.serdes;

import kafka.protocol.Message;
import kafka.protocol.io.DataOutput;
import kafka.util.function.UnsafeBiConsumer;
import lombok.SneakyThrows;

public interface MessageSerializer<T> {

	Class<T> type();

	void serialize(DataOutput output, T message);

	public static <T extends Message> MessageSerializer<T> of(Class<T> type, UnsafeBiConsumer<DataOutput, T> serializer) {
		return new MessageSerializer<T>() {

			@Override
			public Class<T> type() {
				return type;
			}

			@SneakyThrows
			@Override
			public void serialize(DataOutput output, T message) {
				serializer.accept(output, message);
			}

		};
	}

}