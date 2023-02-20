package opwvhk.avro.io;

public class ResolvingFailure extends RuntimeException {
	public ResolvingFailure(String message) {
		super(message);
	}

	public ResolvingFailure(String message, Throwable cause) {
		super(message, cause);
	}
}
