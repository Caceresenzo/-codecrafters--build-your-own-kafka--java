package kafka.protocol.serdes;

import java.io.DataOutput;

public interface MessageSerializer<T> {

	void serialize(DataOutput output, T message);

}