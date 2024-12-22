package kafka;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import kafka.message.apiversions.ApiVersionsRequestV4;
import kafka.message.apiversions.ApiVersionsResponseV4;
import kafka.message.describetopic.DescribeTopicPartitionsRequestV0;
import kafka.message.describetopic.DescribeTopicPartitionsResponseV0;
import kafka.protocol.ErrorCode;
import kafka.protocol.ExchangeMapper;
import kafka.protocol.Header;
import kafka.protocol.ProtocolException;
import kafka.protocol.Request;
import kafka.protocol.Response;
import kafka.protocol.io.DataInputStream;
import kafka.protocol.io.DataOutputStream;
import kafka.record.Batch;
import kafka.record.Record;
import lombok.Getter;
import lombok.SneakyThrows;

@Getter
public class Client implements Runnable {

	private final ExchangeMapper mapper;
	private final Socket socket;
	private final DataInputStream inputStream;
	private final DataOutputStream outputStream;

	public Client(ExchangeMapper mapper, Socket socket) throws IOException {
		this.mapper = mapper;
		this.socket = socket;

		this.inputStream = new DataInputStream(socket.getInputStream());
		this.outputStream = new DataOutputStream(socket.getOutputStream());
	}

	@SneakyThrows
	@Override
	public void run() {
		try (socket) {
			while (!socket.isClosed()) {
				exchange();
			}
		} catch (Exception exception) {
			System.err.println("%s: %s".formatted(socket.getLocalSocketAddress(), exception.getMessage()));

			if (!(exception instanceof EOFException)) {
				exception.printStackTrace();
			}
		}
	}

	private void exchange() {
		try {
			final var request = mapper.receiveRequest(inputStream);
			final var correlationId = request.header().correlationId();

			final var response = handle(request);
			if (response == null) {
				throw new ProtocolException(ErrorCode.UNKNOWN_SERVER_ERROR, correlationId);
			}

			mapper.sendResponse(outputStream, response);
		} catch (ProtocolException exception) {
			mapper.sendErrorResponse(outputStream, exception.correlationId(), exception.code());
		}
	}

	private Response handle(Request request) {
		return switch (request.body()) {
			case ApiVersionsRequestV4 apiVersionsRequest -> new Response(
				new Header.V0(request.header().correlationId()),
				handleApiVersionsRequest(apiVersionsRequest)
			);

			case DescribeTopicPartitionsRequestV0 describeTopicPartitionsRequest -> new Response(
				new Header.V1(request.header().correlationId()),
				handleDescribeTopicPartitionsRequest(describeTopicPartitionsRequest)
			);

			default -> null;
		};
	}

	private ApiVersionsResponseV4 handleApiVersionsRequest(ApiVersionsRequestV4 request) {
		final var keys = mapper.requestApis()
			.stream()
			.map((requestApi) -> new ApiVersionsResponseV4.Key(
				requestApi.key(),
				requestApi.version(),
				requestApi.version())
			)
			.toList();

		return new ApiVersionsResponseV4(
			keys,
			Duration.ZERO
		);
	}

	@SneakyThrows
	private DescribeTopicPartitionsResponseV0 handleDescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestV0 request) {
		final var path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
		try (final var fileInputStream = new FileInputStream(path)) {
			System.out.println(HexFormat.ofDelimiter("").formatHex(fileInputStream.readAllBytes()));
		}

		final var topicRecords = new ArrayList<Record.Topic>();
		final var partitionRecords = new ArrayList<Record.Partition>();
		try (final var fileInputStream = new FileInputStream(path)) {
			final var input = new DataInputStream(fileInputStream);

			while (fileInputStream.available() != 0) {
				final var batch = Batch.deserialize(input);
				System.out.println(batch);

				for (final var record : batch.records()) {
					if (record instanceof Record.Topic topic) {
						topicRecords.add(topic);
					} else if (record instanceof Record.Partition partition) {
						partitionRecords.add(partition);
					}
				}
			}
		}

		final var topicRecordPerName = topicRecords
			.stream()
			.collect(Collectors.toMap(
				Record.Topic::name,
				Function.identity()
			));

		final var partitionRecordsPerTopicId = partitionRecords
			.stream()
			.sorted(Comparator.comparing(Record.Partition::topicId))
			.collect(Collectors.groupingBy(
				Record.Partition::topicId
			));

		final var topicResponses = new ArrayList<DescribeTopicPartitionsResponseV0.Topic>();

		for (final var topicRequest : request.topics()) {
			final var topicRecord = topicRecordPerName.get(topicRequest.name());

			if (topicRecord == null) {
				topicResponses.add(new DescribeTopicPartitionsResponseV0.Topic(
					ErrorCode.UNKNOWN_TOPIC,
					topicRequest.name(),
					UUID.fromString("00000000-0000-0000-0000-000000000000"),
					false,
					Collections.emptyList(),
					0
				));

				continue;
			}

			final var partitionReponses = new ArrayList<DescribeTopicPartitionsResponseV0.Topic.Partition>();
			for (final var partitionRecord : partitionRecordsPerTopicId.getOrDefault(topicRecord.id(), Collections.emptyList())) {
				partitionReponses.add(new DescribeTopicPartitionsResponseV0.Topic.Partition(
					ErrorCode.NONE,
					partitionRecord.id(),
					partitionRecord.leader(),
					partitionRecord.leaderEpoch(),
					partitionRecord.replicas(),
					partitionRecord.inSyncReplicas(),
					partitionRecord.addingReplicas(),
					Collections.emptyList(),
					partitionRecord.removingReplicas()
				));
			}

			topicResponses.add(new DescribeTopicPartitionsResponseV0.Topic(
				ErrorCode.NONE,
				topicRequest.name(),
				topicRecord.id(),
				false,
				partitionReponses,
				0
			));
		}

		return new DescribeTopicPartitionsResponseV0(
			Duration.ZERO,
			topicResponses,
			null
		);
	}

}