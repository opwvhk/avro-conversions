package opwvhk.avro.io;

import java.util.function.Function;

/**
 * A resolver for scalar values. Supports content, but ignores any properties.
 */
public class ScalarValueResolver
		extends ValueResolver {
	private final Function<String, ?> converter;

	/**
	 * Create a scalar value resolver.
	 *
	 * @param converter a converter for string values
	 */
	public ScalarValueResolver(Function<String, ?> converter) {
		this.converter = converter;
	}

	@Override
	public Object addContent(Object collector, String content) {
		return content == null ? null : converter.apply(content);
	}
}
