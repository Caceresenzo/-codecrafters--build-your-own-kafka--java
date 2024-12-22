package kafka.message.describetopic;

import java.util.List;

import kafka.protocol.RequestMessage;

public record DescribeTopicPartitionsRequest(
	List<DescribeTopicPartitionsRequest.Topic> topics,
	int responsePartitionLimit,
	DescribeTopicPartitionsCursor nextCursor
) implements RequestMessage {

	public record Topic(
		String name
	) {}

}