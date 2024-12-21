package kafka.protocol.io;

public interface DataOutput {

	void writeByte(byte value);

	void writeInt(int value);

}