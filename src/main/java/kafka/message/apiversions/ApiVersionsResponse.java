package kafka.message.apiversions;

import java.time.Duration;
import java.util.List;

import kafka.protocol.ResponseMessage;

public record ApiVersionsResponse(
	List<ApiVersionsResponse.Key> apiKeys,
	Duration throttleTime
) implements ResponseMessage {

	public record Key(
		short apiKey,
		short minVersion,
		short maxVersion
	) {}

}