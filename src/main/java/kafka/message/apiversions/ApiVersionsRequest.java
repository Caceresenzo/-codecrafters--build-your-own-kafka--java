package kafka.message.apiversions;

import kafka.protocol.RequestMessage;

public record ApiVersionsRequest(
	ClientSoftware clientSoftware
) implements RequestMessage {

	public record ClientSoftware(
		String name,
		String version
	) {}

}