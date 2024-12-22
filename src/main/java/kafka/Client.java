package kafka;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;

import kafka.message.apiversions.ApiVersionsRequest;
import kafka.message.apiversions.ApiVersionsResponse;
import kafka.message.describetopic.DescribeTopicPartitionsRequest;
import kafka.protocol.ErrorCode;
import kafka.protocol.ExchangeMapper;
import kafka.protocol.ProtocolException;
import kafka.protocol.Request;
import kafka.protocol.ResponseMessage;
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

			mapper.sendResponse(outputStream, correlationId, response);
		} catch (ProtocolException exception) {
			mapper.sendErrorResponse(outputStream, exception.correlationId(), exception.code());
		}
	}

	private ResponseMessage handle(Request<?, ?> request) {
		return switch (request.body()) {
			case ApiVersionsRequest apiVersionsRequest -> new ApiVersionsResponse(
				mapper.deserializers().keySet()
					.stream()
					.map((requestApi) -> new ApiVersionsResponse.Key(
						requestApi.key(),
						requestApi.version(),
						requestApi.version())
					)
					.toList(),
				Duration.ZERO
			);

			case DescribeTopicPartitionsRequest describeTopicPartitionsRequest -> {
				throw new UnsupportedOperationException();
			}

			default -> null;
		};
	}

}