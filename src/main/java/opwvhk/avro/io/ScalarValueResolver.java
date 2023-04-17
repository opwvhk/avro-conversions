package opwvhk.avro.io;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A resolver for scalar values. Supports content, but ignores any properties.
 */
public class ScalarValueResolver
		extends ValueResolver {
	private final Function<String, Object> converter;

	/**
	 * Create a scalar value resolver.
	 *
	 * @param converter a converter for string values
	 */
	public ScalarValueResolver(Function<String, Object> converter) {
		this.converter = converter;
	}

	@Override
	public Object addContent(Object collector, String content) {
		return content == null ? null : converter.apply(content);
	}
}
