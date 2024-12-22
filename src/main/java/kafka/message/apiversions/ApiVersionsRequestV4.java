package kafka.message.apiversions;

import kafka.protocol.RequestApi;
import kafka.protocol.RequestBody;
import kafka.protocol.io.DataInput;

public record ApiVersionsRequestV4(
	ClientSoftware clientSoftware
) implements RequestBody {

	public static final RequestApi API = RequestApi.of(18, 4);

	public static ApiVersionsRequestV4 deserialize(DataInput input) {
		final var clientSoftwareName = input.readCompactString();
		final var clientSoftwareVersion = input.readCompactString();

		input.skipEmptyTaggedFieldArray();

		return new ApiVersionsRequestV4(new ClientSoftware(
			clientSoftwareName,
			clientSoftwareVersion
		));
	}

	public record ClientSoftware(
		String name,
		String version
	) {}

}