package kafka.protocol;

public record Request(
	Header header,
	RequestBody body
) {}