package kafka.protocol;

import java.util.Optional;

import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;

public sealed interface Header {

	int correlationId();

	void serialize(DataOutput output);

	public record V0(
		int correlationId
	) implements Header {

		@Override
		public void serialize(DataOutput output) {
			output.writeInt(correlationId);
		}

	}

	public record V1(
		int correlationId
	) implements Header {

		@Override
		public void serialize(DataOutput output) {
			output.writeInt(correlationId);

			output.skipEmptyTaggedFieldArray();
		}

	}

	public record V2(
		RequestApi requestApi,
		int correlationId,
		Optional<String> clientId
	) implements Header {

		@Override
		public void serialize(DataOutput output) {
			throw new UnsupportedOperationException();
		}

		public static V2 deserialize(DataInput input) {
			final var requestApi = RequestApi.of(
				input.readSignedShort(),
				input.readSignedShort()
			);

			final var correlationId = input.readSignedInt();
			final var clientId = input.readString();

			input.skipEmptyTaggedFieldArray();

			final var header = new V2(requestApi, correlationId, Optional.of(clientId));
			if (requestApi.version() < 0 || requestApi.version() > 4) {
				throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, correlationId);
			}

			return header;
		}

	}

}