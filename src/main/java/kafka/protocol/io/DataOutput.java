package kafka.protocol.io;

import java.util.List;
import java.util.function.BiConsumer;

public interface DataOutput {

	void writeBytes(byte[] bytes);
	
	void writeByte(byte value);

	void writeShort(short value);

	void writeInt(int value);

	default void writeUnsignedVarint(long value) {
		VarInt.writeLong(value, this);
	}

	default <T> void writeCompactArray(List<T> items, BiConsumer<DataOutput, T> serializer) {
		if (items == null) {
			writeUnsignedVarint(0);
			return;
		}

		writeUnsignedVarint(items.size() + 1);

		for (final var item : items) {
			serializer.accept(this, item);
		}
	}

	default void skipEmptyTaggedFieldArray() {
		writeUnsignedVarint(0);
	}

}