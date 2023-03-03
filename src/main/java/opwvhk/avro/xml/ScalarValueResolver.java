package opwvhk.avro.xml;

import java.util.function.Function;

public class ScalarValueResolver
		extends ValueResolver {
	private final Function<String, Object> converter;

	public ScalarValueResolver(Function<String, Object> converter) {
		this.converter = converter;
	}

	@Override
	public Object addContent(Object collector, String content) {
		return converter.apply(content);
	}
}
