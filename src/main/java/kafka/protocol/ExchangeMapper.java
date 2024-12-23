package kafka.protocol;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import kafka.message.apiversions.ApiVersionsRequestV4;
import kafka.message.describetopic.DescribeTopicPartitionsRequestV0;
import kafka.message.fetch.FetchRequestV16;
import kafka.protocol.io.DataByteBuffer;
import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;
import kafka.protocol.io.DataOutputStream;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class ExchangeMapper {

	private final Map<RequestApi, Function<DataInput, ? extends RequestBody>> deserializers = new HashMap<>();

	public ExchangeMapper() {
		deserializers.put(FetchRequestV16.API, FetchRequestV16::deserialize);
		deserializers.put(ApiVersionsRequestV4.API, ApiVersionsRequestV4::deserialize);
		deserializers.put(DescribeTopicPartitionsRequestV0.API, DescribeTopicPartitionsRequestV0::deserialize);
	}

	public Request receiveRequest(DataInput input) {
		final var messageSize = input.readSignedInt();
		final var buffer = input.readNBytes(messageSize);
		input = new DataByteBuffer(buffer);

		final var header = Header.V2.deserialize(input);

		final var deserializer = getDeserializer(header);
		final var body = deserializer.apply(input);

		return new Request(header, body);
	}

	public void sendResponse(DataOutput output, Response response) {
		final var byteOutputStream = new ByteArrayOutputStream();

		final var temporaryOutput = new DataOutputStream(byteOutputStream);
		response.serialize(temporaryOutput);

		final var bytes = byteOutputStream.toByteArray();

		output.writeInt(bytes.length);
		output.writeRawBytes(bytes);
	}

	private Function<DataInput, ? extends RequestBody> getDeserializer(Header.V2 header) {
		final var deserializer = deserializers.get(header.requestApi());
		if (deserializer == null) {
			throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, header.correlationId());
		}

		return deserializer;
	}

	public void sendErrorResponse(DataOutput output, int correlationId, ErrorCode errorCode) {
		output.writeInt(4 + 2);
		output.writeInt(correlationId);
		output.writeShort(errorCode.value());
	}

	public List<RequestApi> requestApis() {
		return new ArrayList<>(deserializers.keySet());
	}

}