package kafka.protocol.message;

import kafka.message.describetopic.DescribeTopicPartitionsCursor;
import kafka.message.describetopic.DescribeTopicPartitionsRequest;
import kafka.message.describetopic.DescribeTopicPartitionsResponse;
import kafka.protocol.ErrorCode;
import kafka.protocol.RequestApi;
import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;
import kafka.protocol.serdes.MessageDeserializer;
import kafka.protocol.serdes.MessageSerializer;

public class DescribeTopicPartitionsV0Serdes implements MessageDeserializer<DescribeTopicPartitionsRequest>, MessageSerializer<DescribeTopicPartitionsResponse> {

	@Override
	public RequestApi requestApi() {
		return RequestApi.of(75, 0);
	}

	@Override
	public DescribeTopicPartitionsRequest deserialize(DataInput input) {
		final var topics = input.<DescribeTopicPartitionsRequest.Topic>readCompactArray(this::deserializeTopic);
		final var responsePartitionLimit = input.readSignedInt();
		final var nextCursor = deserializeCursor(input);

		return new DescribeTopicPartitionsRequest(
			topics,
			responsePartitionLimit,
			nextCursor
		);
	}

	public DescribeTopicPartitionsRequest.Topic deserializeTopic(DataInput input) {
		final var name = input.readCompactString();

		input.skipEmptyTaggedFieldArray();

		return new DescribeTopicPartitionsRequest.Topic(
			name
		);
	}

	public DescribeTopicPartitionsCursor deserializeCursor(DataInput input) {
		final var topicName = input.readCompactString();
		final var partitionIndex = input.readSignedInt();

		input.skipEmptyTaggedFieldArray();

		return new DescribeTopicPartitionsCursor(
			topicName,
			partitionIndex
		);
	}

	@Override
	public Class<DescribeTopicPartitionsResponse> type() {
		return DescribeTopicPartitionsResponse.class;
	}

	@Override
	public void serialize(DataOutput output, DescribeTopicPartitionsResponse message) {
		output.writeInt((int) message.throttleTime().toMillis());
		output.writeCompactArray(message.topics(), this::serialize);
		serialize(output, message.nextCursor());

		output.skipEmptyTaggedFieldArray();
	}

	public void serialize(DataOutput output, DescribeTopicPartitionsResponse.Topic message) {
		output.writeShort(message.errorCode().value());

		if (!ErrorCode.NONE.equals(message.errorCode())) {
			return;
		}

		output.writeCompactString(message.name());
		output.writeUuid(message.topicId());
		output.writeBoolean(message.isInternal());
		output.writeCompactArray(message.partitions(), this::serialize);
		output.writeInt(message.topicAuthorizedOperations());
	}

	public void serialize(DataOutput output, DescribeTopicPartitionsResponse.Topic.Partition message) {
		output.writeShort(message.errorCode().value());

		if (!ErrorCode.NONE.equals(message.errorCode())) {
			return;
		}

		output.writeInt(message.partitionIndex());
		output.writeInt(message.leaderId());
		output.writeInt(message.leaderEpoch());
		output.writeCompactArray(message.replicaNodes(), DataOutput::writeInt);
		output.writeCompactArray(message.isrNodes(), DataOutput::writeInt);
		output.writeCompactArray(message.eligibleLeaderReplicas(), DataOutput::writeInt);
		output.writeCompactArray(message.lastKnownElr(), DataOutput::writeInt);
		output.writeCompactArray(message.offlineReplicas(), DataOutput::writeInt);
	}

	public void serialize(DataOutput output, DescribeTopicPartitionsCursor message) {
		output.writeCompactString(message.topicName());
		output.writeInt(message.partitionIndex());

		output.skipEmptyTaggedFieldArray();
	}

}