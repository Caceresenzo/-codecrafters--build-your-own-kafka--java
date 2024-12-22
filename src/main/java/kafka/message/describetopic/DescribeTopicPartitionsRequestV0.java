package kafka.message.describetopic;

import java.util.List;

import kafka.protocol.RequestApi;
import kafka.protocol.RequestBody;
import kafka.protocol.io.DataInput;

public record DescribeTopicPartitionsRequestV0(
	List<DescribeTopicPartitionsRequestV0.Topic> topics,
	int responsePartitionLimit,
	DescribeTopicPartitionsCursorV0 nextCursor
) implements RequestBody {

	public static final RequestApi API = RequestApi.of(75, 0);

	public static DescribeTopicPartitionsRequestV0 deserialize(DataInput input) {
		final var topics = input.readCompactArray(DescribeTopicPartitionsRequestV0.Topic::deserialize);
		final var responsePartitionLimit = input.readSignedInt();
		final var nextCursor = DescribeTopicPartitionsCursorV0.deserialize(input);

		return new DescribeTopicPartitionsRequestV0(
			topics,
			responsePartitionLimit,
			nextCursor
		);
	}

	public record Topic(
		String name
	) {

		public static DescribeTopicPartitionsRequestV0.Topic deserialize(DataInput input) {
			final var name = input.readCompactString();

			input.skipEmptyTaggedFieldArray();

			return new DescribeTopicPartitionsRequestV0.Topic(
				name
			);
		}

	}

}