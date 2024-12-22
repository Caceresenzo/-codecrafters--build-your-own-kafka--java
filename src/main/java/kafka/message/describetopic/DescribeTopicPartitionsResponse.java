package kafka.message.describetopic;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import kafka.protocol.ErrorCode;
import kafka.protocol.ResponseMessage;

public record DescribeTopicPartitionsResponse(
	Duration throttleTime,
	List<DescribeTopicPartitionsResponse.Topic> topics,
	DescribeTopicPartitionsCursor nextCursor
) implements ResponseMessage {

	public record Topic(
		ErrorCode errorCode,
		String name,
		UUID topicId,
		boolean isInternal,
		List<DescribeTopicPartitionsResponse.Topic.Partition> partitions,
		int topicAuthorizedOperations
	) {

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
		) {}

	}

}