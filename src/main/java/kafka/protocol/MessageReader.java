package kafka.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import kafka.Client;
import kafka.message.apiversions.ApiVersionsRequest;
import kafka.protocol.io.DataByteBuffer;
import kafka.protocol.serdes.MessageDeserializer;

public class MessageReader {

	private final Map<RequestApi, MessageDeserializer<?>> deserializers = new HashMap<>();

	public MessageReader() {
		addDeserializer(MessageDeserializer.<ApiVersionsRequest>of(RequestApi.of(18, 4), (input) -> {
			input.skipEmptyTaggedFieldArray();

			final var clientSoftwareName = input.readCompactString();
			final var clientSoftwareVersion = input.readCompactString();

			input.skipEmptyTaggedFieldArray();

			return new ApiVersionsRequest(new ApiVersionsRequest.ClientSoftware(
				clientSoftwareName,
				clientSoftwareVersion
			));
		}));
	}

	private void addDeserializer(MessageDeserializer<?> deserializer) {
		deserializers.put(deserializer.requestApi(), deserializer);
	}

	public Request<?, ?> next(Client client) {
		final var inputStream = client.getInputStream();

		final var messageSize = inputStream.readSignedInt();
		final var byteBuffer = inputStream.readNBytes(messageSize);

		final var reader = new DataByteBuffer(byteBuffer);

		final var requestApi = RequestApi.of(
			reader.readSignedShort(),
			reader.readSignedShort()
		);

		final var correlationId = reader.readSignedInt();
		final var clientId = reader.readString();

		final var header = new HeaderV2(requestApi, correlationId, Optional.of(clientId));
		if (requestApi.version() < 0 || requestApi.version() > 4) {
			throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, correlationId);
		}

		final var deserializer = deserializers.get(requestApi);
		if (deserializer == null) {
			throw new IllegalStateException("unknown request api: %s".formatted(requestApi));
		}

		final var body = deserializer.deserialize(reader);

		return new Request<>(header, body);
	}

}