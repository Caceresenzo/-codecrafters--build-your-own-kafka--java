package kafka;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

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

	private DescribeTopicPartitionsResponseV0 handleDescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestV0 request) {
		final var topics = new ArrayList<DescribeTopicPartitionsResponseV0.Topic>();

		for (final var topic : request.topics()) {
			topics.add(new DescribeTopicPartitionsResponseV0.Topic(
				ErrorCode.UNKNOWN_TOPIC,
				topic.name(),
				UUID.fromString("00000000-0000-0000-0000-000000000000"),
				false,
				Collections.emptyList(),
				0
			));
		}

		return new DescribeTopicPartitionsResponseV0(
			Duration.ZERO,
			topics,
			null
		);
	}

}