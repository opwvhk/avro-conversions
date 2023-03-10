package opwvhk.avro.xml;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class ScalarValueResolver
		extends ValueResolver {
	private final Function<String, Object> converter;

	ScalarValueResolver(Function<String, Object> converter) {
		this.converter = converter;
	}

	@Override
	public Object addContent(Object collector, String content) {
		return converter.apply(requireNonNull(content));
	}
}
