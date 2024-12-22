package kafka.util.function;

@FunctionalInterface
public interface UnsafeBiConsumer<T, U> {

	void accept(T t, U u) throws Throwable;

}