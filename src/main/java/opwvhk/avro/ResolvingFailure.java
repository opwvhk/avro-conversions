package opwvhk.avro;

/**
 * Runtime exception for resolving failures. Used to indicate a failure to resolve an Avro schema and/or an X-to-Avro parser.
 */
public class ResolvingFailure extends RuntimeException {
	/**
	 * Create a resolving failure for the specified reason.
	 *
	 * @param message the reason resolving failed
	 */
	public ResolvingFailure(String message) {
		super(message);
	}
}
