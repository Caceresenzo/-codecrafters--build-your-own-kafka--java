package kafka;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

	public static final int PORT = 9092;

	public static void main(String[] args) {
		Socket clientSocket = null;

		System.out.println("listen: %d".formatted(PORT));
		try (
			final var serverSocket = new ServerSocket(PORT)
		) {
			serverSocket.setReuseAddress(true);

			clientSocket = serverSocket.accept();
			System.out.println("connected: %s".formatted(clientSocket.getRemoteSocketAddress()));

			final var client = new Client(clientSocket);
			client.run();

			clientSocket.close();
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		} finally {
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (IOException e) {
				System.out.println("IOException: " + e.getMessage());
			}
		}
	}

}