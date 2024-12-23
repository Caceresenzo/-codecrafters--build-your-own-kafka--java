package kafka.protocol.io;

import java.io.OutputStream;

import lombok.SneakyThrows;

public class DataOutputStream implements DataOutput, AutoCloseable {

	private final java.io.DataOutputStream delegate;

	public DataOutputStream(OutputStream outputStream) {
		this.delegate = new java.io.DataOutputStream(outputStream);
	}

	@SneakyThrows
	@Override
	public void writeRawBytes(byte[] bytes) {
		delegate.write(bytes);
	}

	@SneakyThrows
	@Override
	public void writeByte(byte value) {
		delegate.writeByte(value);
	}

	@SneakyThrows
	@Override
	public void writeShort(short value) {
		delegate.writeShort(value);
	}

	@SneakyThrows
	@Override
	public void writeInt(int value) {
		delegate.writeInt(value);
	}

	@SneakyThrows
	@Override
	public void writeLong(long value) {
		delegate.writeLong(value);
	}

	@SneakyThrows
	@Override
	public void close() {
		delegate.close();
	}

}