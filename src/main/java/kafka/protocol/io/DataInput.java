package kafka.protocol.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface DataInput {

	ByteBuffer readNBytes(int n);

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

	default void skipEmptyTaggedFieldArray() {
		readUnsignedVarint();
	}

}