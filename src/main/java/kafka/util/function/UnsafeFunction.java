package kafka.util.function;

@FunctionalInterface
public interface UnsafeFunction<T, R> {
	
	R apply(T t) throws Throwable;
	
	public static <T> UnsafeFunction<T, T> identity() {
		return (x) -> x;
	}
	
}