package kafka.protocol;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import kafka.protocol.io.DataByteBuffer;
import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;
import kafka.protocol.io.DataOutputStream;
import kafka.protocol.message.ApiVersionsV4Serdes;
import kafka.protocol.serdes.MessageDeserializer;
import kafka.protocol.serdes.MessageSerializer;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class ExchangeMapper {

	private final Map<RequestApi, MessageDeserializer<?>> deserializers = new HashMap<>();
	private final Map<Class<? extends ResponseMessage>, MessageSerializer<?>> serializers = new HashMap<>();

	public ExchangeMapper() {
		final var apiVersionsV4 = new ApiVersionsV4Serdes();

		addDeserializer(apiVersionsV4);
		addSerializer(apiVersionsV4);
	}

	private void addDeserializer(MessageDeserializer<?> deserializer) {
		deserializers.put(deserializer.requestApi(), deserializer);
	}

	private void addSerializer(MessageSerializer<? extends ResponseMessage> serializer) {
		serializers.put(serializer.type(), serializer);
	}

	public Request<?, ?> receiveRequest(DataInput input) {
		final var messageSize = input.readSignedInt();
		input = new DataByteBuffer(input.readNBytes(messageSize));

		final var requestApi = RequestApi.of(
			input.readSignedShort(),
			input.readSignedShort()
		);

		final var correlationId = input.readSignedInt();
		final var clientId = input.readString();

		final var header = new HeaderV2(requestApi, correlationId, Optional.of(clientId));
		if (requestApi.version() < 0 || requestApi.version() > 4) {
			throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, correlationId);
		}

		final var deserializer = deserializers.get(requestApi);
		if (deserializer == null) {
			throw new IllegalStateException("unknown request api: %s".formatted(requestApi));
		}

		final var body = deserializer.deserialize(input);

		return new Request<>(header, body);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void sendResponse(DataOutput output, int correlationId, ResponseMessage response) {
		final var responseType = response.getClass();

		final MessageSerializer serializer = serializers.get(responseType);
		if (serializer == null) {
			throw new IllegalStateException("unknown response type: %s".formatted(responseType));
		}

		final var byteOutputStream = new ByteArrayOutputStream();
		serializer.serialize(new DataOutputStream(byteOutputStream), response);

		System.out.println(Arrays.toString(byteOutputStream.toByteArray()));

		doSendResponse(output, correlationId, ErrorCode.NONE, byteOutputStream.toByteArray());
	}

	public void sendErrorResponse(DataOutput output, int correlationId, ErrorCode errorCode) {
		doSendResponse(output, correlationId, errorCode, new byte[0]);
	}

	public void doSendResponse(DataOutput output, int correlationId, ErrorCode errorCode, byte[] bytes) {
		output.writeInt(4 + 2 + bytes.length);
		output.writeInt(correlationId);
		output.writeShort(errorCode.value());
		output.writeBytes(bytes);
	}

}