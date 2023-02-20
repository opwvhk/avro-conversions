package opwvhk.avro.io;

import java.util.ArrayList;
import java.util.List;

class ListResolver extends ValueResolver {
	private final ValueResolver resolver;

	ListResolver(ValueResolver resolver) {
		this.resolver = resolver;
	}

	@Override
	public ValueResolver resolve(String name) {
		return resolver;
	}

	@Override
	public Object createCollector() {
		return new ArrayList<>();
	}

	@Override
	public Object addProperty(Object collector, String name, Object value) {
		//noinspection unchecked
		((List<Object>)collector).add(value);
		return collector;
	}
}
