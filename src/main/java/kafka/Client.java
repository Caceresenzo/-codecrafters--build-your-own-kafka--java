package kafka;

import java.io.IOException;
import java.net.Socket;

import kafka.protocol.MessageReader;
import kafka.protocol.ProtocolException;
import kafka.protocol.io.DataInputStream;
import kafka.protocol.io.DataOutputStream;
import lombok.Getter;

@Getter
public class Client {

	private final Socket socket;
	private final DataInputStream inputStream;
	private final DataOutputStream outputStream;

	public Client(Socket socket) throws IOException {
		super();
		this.socket = socket;

		this.inputStream = new DataInputStream(socket.getInputStream());
		this.outputStream = new DataOutputStream(socket.getOutputStream());
	}

	public void run() {
		final var reader = new MessageReader();

		try {
			final var request = reader.next(this);
			System.out.println(request);

			outputStream.writeInt(4);
			outputStream.writeInt(request.header().correlationId());
		} catch (ProtocolException exception) {
			outputStream.writeInt(6);
			outputStream.writeInt(exception.correlationId());
			outputStream.writeShort(exception.code().value());
		}
	}

}