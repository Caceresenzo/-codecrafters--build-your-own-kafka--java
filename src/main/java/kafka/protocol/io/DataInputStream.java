package kafka.protocol.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

import lombok.SneakyThrows;

public class DataInputStream implements DataInput {

	private final java.io.DataInputStream delegate;

	public DataInputStream(InputStream in) {
		this.delegate = new java.io.DataInputStream(in);
	}

	@SneakyThrows
	public ByteBuffer readNBytes(int n) {
		return ByteBuffer.wrap(delegate.readNBytes(n));
	}

	@SneakyThrows
	@Override
	public byte readByte() {
		return delegate.readByte();
	}

	@SneakyThrows
	@Override
	public short readSignedShort() {
		return delegate.readShort();
	}

	@SneakyThrows
	@Override
	public int readSignedInt() {
		return delegate.readInt();
	}

}