package kafka.protocol;

public record Request<H extends Header, M extends Message>(
	H header,
	M body
) {}