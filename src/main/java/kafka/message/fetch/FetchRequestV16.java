package kafka.message.fetch;

import kafka.protocol.RequestApi;
import kafka.protocol.RequestBody;
import kafka.protocol.io.DataInput;

public record FetchRequestV16() implements RequestBody {

	public static final RequestApi API = RequestApi.of(1, 16);

	public static FetchRequestV16 deserialize(DataInput input) {
		throw new UnsupportedOperationException();
	}

}