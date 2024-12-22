package kafka;

import java.net.ServerSocket;

import kafka.protocol.ExchangeMapper;
import lombok.SneakyThrows;

public class Main {

	public static final int PORT = 9092;

	@SneakyThrows
	public static void main(String[] args) {
		final var exchangeMapper = new ExchangeMapper();

		System.out.println("listen: %d".formatted(PORT));
		try (final var serverSocket = new ServerSocket(PORT)) {
			serverSocket.setReuseAddress(true);

			while (true) {
				final var clientSocket = serverSocket.accept();
				System.out.println("connected: %s".formatted(clientSocket.getRemoteSocketAddress()));

				Thread.ofVirtual().start(new Client(exchangeMapper, clientSocket));
			}
		}
	}

}