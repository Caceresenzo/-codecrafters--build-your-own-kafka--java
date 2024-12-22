package kafka.protocol.message;

import kafka.message.apiversions.ApiVersionsRequest;
import kafka.message.apiversions.ApiVersionsResponse;
import kafka.protocol.RequestApi;
import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;
import kafka.protocol.serdes.MessageDeserializer;
import kafka.protocol.serdes.MessageSerializer;

public class ApiVersionsV4Serdes implements MessageDeserializer<ApiVersionsRequest>, MessageSerializer<ApiVersionsResponse> {

	@Override
	public RequestApi requestApi() {
		return RequestApi.of(18, 4);
	}

	@Override
	public ApiVersionsRequest deserialize(DataInput input) {
		input.skipEmptyTaggedFieldArray();

		final var clientSoftwareName = input.readCompactString();
		final var clientSoftwareVersion = input.readCompactString();

		input.skipEmptyTaggedFieldArray();

		return new ApiVersionsRequest(new ApiVersionsRequest.ClientSoftware(
			clientSoftwareName,
			clientSoftwareVersion
		));
	}

	@Override
	public Class<ApiVersionsResponse> type() {
		return ApiVersionsResponse.class;
	}

	@Override
	public void serialize(DataOutput output, ApiVersionsResponse message) {
		output.writeCompactArray(message.apiKeys(), this::serialize);
		output.writeInt((int) message.throttleTime().toMillis());

		output.skipEmptyTaggedFieldArray();
	}

	public void serialize(DataOutput output, ApiVersionsResponse.Key message) {
		output.writeShort(message.apiKey());
		output.writeShort(message.minVersion());
		output.writeShort(message.maxVersion());

		output.skipEmptyTaggedFieldArray();
	}

}