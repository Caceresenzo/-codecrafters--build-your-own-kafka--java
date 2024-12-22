package kafka;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;

import kafka.message.apiversions.ApiVersionsRequest;
import kafka.message.apiversions.ApiVersionsResponse;
import kafka.protocol.ErrorCode;
import kafka.protocol.ExchangeMapper;
import kafka.protocol.ProtocolException;
import kafka.protocol.Request;
import kafka.protocol.ResponseMessage;
import kafka.protocol.io.DataInputStream;
import kafka.protocol.io.DataOutputStream;
import lombok.Getter;

@Getter
public class Client {

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

	public void run() {

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

	public ResponseMessage handle(Request<?, ?> request) {
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

			default -> null;
		};
	}

}