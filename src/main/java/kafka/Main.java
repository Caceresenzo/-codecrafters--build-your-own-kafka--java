package kafka;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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

			clientSocket.getInputStream().read();

			final var responseOutputStream = new ByteArrayOutputStream();
			final var dataOutputStream = new DataOutputStream(responseOutputStream);

			dataOutputStream.writeInt(0);
			dataOutputStream.writeInt(7);

			clientSocket.getOutputStream().write(responseOutputStream.toByteArray());
			clientSocket.getOutputStream().flush();

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