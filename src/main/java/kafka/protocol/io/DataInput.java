package kafka.protocol.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public interface DataInput {

	ByteBuffer readNBytes(int n);

	byte peekByte();

	byte readByte();

	short readSignedShort();

	int readSignedInt();

	default long readUnsignedVarint() {
		return VarInt.readLong(this);
	}

	default String readString() {
		final var length = readSignedShort();

		if (length == -1) {
			return null;
		}

		final var bytes = readNBytes(length);
		return new String(bytes.array(), bytes.arrayOffset(), bytes.limit(), StandardCharsets.UTF_8);
	}

	default String readCompactString() {
		final var length = readUnsignedVarint();

		if (length == 0) {
			return null;
		}

		final var bytes = readNBytes((int) length - 1);
		return new String(bytes.array(), bytes.arrayOffset(), bytes.limit(), StandardCharsets.UTF_8);
	}

	default <T> List<T> readCompactArray(Function<DataInput, T> deserializer) {
		var length = readUnsignedVarint();

		if (length == 0) {
			return null;
		}

		--length;
		final var items = new ArrayList<T>((int) length);

		for (var index = 0; index < length; ++index) {
			items.add(deserializer.apply(this));
		}

		return items;
	}

	default void skipEmptyTaggedFieldArray() {
		readUnsignedVarint();
	}

}