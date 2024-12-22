package kafka.util.function;

@FunctionalInterface
public interface UnsafeBiFunction<T, U, R> {

    R apply(T t, U u) throws Throwable;
	
}