package kafka;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import kafka.protocol.io.DataInputStream;
import kafka.record.Batch;
import kafka.record.Record;
import kafka.record.Record.Partition;
import kafka.record.Record.Topic;
import lombok.SneakyThrows;

public class Kafka {

	private final Map<UUID, Record.Topic> topicPerId;
	private final Map<String, Record.Topic> topicPerName;
	private final Map<UUID, List<Record.Partition>> partitionsPerTopicId;

	public Kafka(List<Topic> topics, List<Partition> records) {
		this.topicPerId = topics
			.stream()
			.collect(Collectors.toMap(
				Record.Topic::id,
				Function.identity()
			));

		this.topicPerName = topics
			.stream()
			.collect(Collectors.toMap(
				Record.Topic::name,
				Function.identity()
			));

		this.partitionsPerTopicId = records
			.stream()
			.sorted(Comparator.comparing(Record.Partition::topicId))
			.collect(Collectors.groupingBy(
				Record.Partition::topicId
			));
	}

	public Record.Topic getTopic(UUID id) {
		return topicPerId.get(id);
	}

	public Record.Topic getTopic(String name) {
		return topicPerName.get(name);
	}

	public List<Record.Partition> getPartitions(UUID topicId) {
		return partitionsPerTopicId.getOrDefault(topicId, Collections.emptyList());
	}

	@SneakyThrows
	public static Kafka load(String path) {
		try (final var fileInputStream = new FileInputStream(path)) {
			System.out.println(HexFormat.ofDelimiter("").formatHex(fileInputStream.readAllBytes()));
		}

		final var topics = new ArrayList<Record.Topic>();
		final var partitions = new ArrayList<Record.Partition>();

		try (final var fileInputStream = new FileInputStream(path)) {
			final var input = new DataInputStream(fileInputStream);

			while (fileInputStream.available() != 0) {
				final var batch = Batch.deserialize(input);
				System.out.println(batch);

				for (final var record : batch.records()) {
					if (record instanceof Record.Topic topic) {
						topics.add(topic);
					} else if (record instanceof Record.Partition partition) {
						partitions.add(partition);
					}
				}
			}
		}

		return new Kafka(topics, partitions);
	}

}