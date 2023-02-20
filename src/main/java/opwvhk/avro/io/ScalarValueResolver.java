package opwvhk.avro.io;

import java.util.function.Function;

import lombok.NonNull;

public class ScalarValueResolver
		extends ValueResolver {
	private final Function<String, Object> converter;

	public ScalarValueResolver(@NonNull Function<String, Object> converter) {
		this.converter = converter;
	}

	@Override
	public Object addContent(Object collector, String content) {
		return converter.apply(content);
	}
}
