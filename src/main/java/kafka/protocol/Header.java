package kafka.protocol;

import java.util.Optional;

public interface Header {

	RequestApi requestApi();

	int correlationId();

	Optional<String> clientId();

}