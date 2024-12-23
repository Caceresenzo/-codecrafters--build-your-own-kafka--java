package kafka;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import kafka.message.apiversions.ApiVersionsRequestV4;
import kafka.message.apiversions.ApiVersionsResponseV4;
import kafka.message.describetopic.DescribeTopicPartitionsRequestV0;
import kafka.message.describetopic.DescribeTopicPartitionsResponseV0;
import kafka.message.fetch.FetchRequestV16;
import kafka.message.fetch.FetchResponseV16;
import kafka.protocol.ErrorCode;
import kafka.protocol.ExchangeMapper;
import kafka.protocol.Header;
import kafka.protocol.ProtocolException;
import kafka.protocol.Request;
import kafka.protocol.Response;
import kafka.protocol.io.DataInputStream;
import kafka.protocol.io.DataOutputStream;
import lombok.Getter;
import lombok.SneakyThrows;

@Getter
public class Client implements Runnable {

	private final Kafka kafka;
	private final ExchangeMapper mapper;
	private final Socket socket;
	private final DataInputStream inputStream;
	private final DataOutputStream outputStream;

	public Client(Kafka kafka, ExchangeMapper mapper, Socket socket) throws IOException {
		this.kafka = kafka;
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

			case FetchRequestV16 fetchRequest -> new Response(
				new Header.V1(request.header().correlationId()),
				handleFetchRequest(fetchRequest)
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
		final var topicResponses = new ArrayList<DescribeTopicPartitionsResponseV0.Topic>();

		for (final var topicRequest : request.topics()) {
			final var topicRecord = kafka.getTopic(topicRequest.name());

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
			for (final var partitionRecord : kafka.getPartitions(topicRecord.id())) {
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

	private FetchResponseV16 handleFetchRequest(FetchRequestV16 request) {
		final var responses = new ArrayList<FetchResponseV16.Response>();

		for (final var topicRequest : request.topics()) {
			final var topicRecord = kafka.getTopic(topicRequest.topicId());

			if (topicRecord == null) {
				responses.add(new FetchResponseV16.Response(
					topicRequest.topicId(),
					List.of(
						new FetchResponseV16.Response.Partition(
							0,
							ErrorCode.UNKNOWN_TOPIC_ID,
							0,
							0,
							0,
							Collections.emptyList(),
							0,
							new byte[0]
						)
					)
				));

				continue;
			}

			responses.add(new FetchResponseV16.Response(
				topicRequest.topicId(),
				List.of(
					new FetchResponseV16.Response.Partition(
						0,
						ErrorCode.NONE,
						0,
						0,
						0,
						Collections.emptyList(),
						0,
						new byte[0]
					)
				)
			));
		}

		return new FetchResponseV16(
			Duration.ZERO,
			ErrorCode.NONE,
			request.sessionId(),
			responses
		);
	}

}