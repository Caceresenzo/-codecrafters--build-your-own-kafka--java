package kafka.message.describetopic;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import kafka.protocol.ErrorCode;
import kafka.protocol.ResponseBody;
import kafka.protocol.io.DataOutput;

public record DescribeTopicPartitionsResponseV0(
	Duration throttleTime,
	List<DescribeTopicPartitionsResponseV0.Topic> topics,
	DescribeTopicPartitionsCursorV0 nextCursor
) implements ResponseBody {

	@Override
	public void serialize(DataOutput output) {
		output.writeInt((int) throttleTime.toMillis());
		output.writeCompactArray(topics, DescribeTopicPartitionsResponseV0.Topic::serialize);
		DescribeTopicPartitionsCursorV0.serialize(nextCursor, output);

		output.skipEmptyTaggedFieldArray();
	}

	public record Topic(
		ErrorCode errorCode,
		String name,
		UUID topicId,
		boolean isInternal,
		List<DescribeTopicPartitionsResponseV0.Topic.Partition> partitions,
		int topicAuthorizedOperations
	) {

		public void serialize(DataOutput output) {
			output.writeShort(errorCode.value());
			output.writeCompactString(name);
			output.writeUuid(topicId);
			output.writeBoolean(isInternal);
			output.writeCompactArray(partitions, DescribeTopicPartitionsResponseV0.Topic.Partition::serialize);
			output.writeInt(topicAuthorizedOperations);

			output.skipEmptyTaggedFieldArray();
		}

		public record Partition(
			ErrorCode errorCode,
			int partitionIndex,
			int leaderId,
			int leaderEpoch,
			List<Integer> replicaNodes,
			List<Integer> isrNodes,
			List<Integer> eligibleLeaderReplicas,
			List<Integer> lastKnownElr,
			List<Integer> offlineReplicas
		) {

			public void serialize(DataOutput output) {
				output.writeShort(errorCode.value());
				output.writeInt(partitionIndex);
				output.writeInt(leaderId);
				output.writeInt(leaderEpoch);
				output.writeCompactIntArray(replicaNodes);
				output.writeCompactIntArray(isrNodes);
				output.writeCompactIntArray(eligibleLeaderReplicas);
				output.writeCompactIntArray(lastKnownElr);
				output.writeCompactIntArray(offlineReplicas);
			}

		}

	}

}