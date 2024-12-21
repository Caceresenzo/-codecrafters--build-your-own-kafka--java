package kafka.protocol;

import java.util.Optional;

public record HeaderV2(
	RequestApi requestApi,
	int correlationId,
	Optional<String> clientId
) implements Header {}