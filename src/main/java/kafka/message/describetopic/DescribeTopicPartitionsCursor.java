package kafka.message.describetopic;


public record DescribeTopicPartitionsCursor(
	String topicName,
	int partitionIndex
) {}