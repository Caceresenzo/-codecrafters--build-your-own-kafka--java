package kafka.message.describetopic;

import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;

public record DescribeTopicPartitionsCursorV0(
	String topicName,
	int partitionIndex
) {

	public static DescribeTopicPartitionsCursorV0 deserialize(DataInput input) {
		if (input.peekByte() == (byte) 0xff) {
			return null;
		}

		final var topicName = input.readCompactString();
		final var partitionIndex = input.readSignedInt();

		input.skipEmptyTaggedFieldArray();

		return new DescribeTopicPartitionsCursorV0(
			topicName,
			partitionIndex
		);
	}

	public static void serialize(DescribeTopicPartitionsCursorV0 cursor, DataOutput output) {
		if (cursor == null) {
			output.writeByte((byte) 0xff);
			return;
		}

		output.writeCompactString(cursor.topicName());
		output.writeInt(cursor.partitionIndex());

		output.skipEmptyTaggedFieldArray();
	}

}