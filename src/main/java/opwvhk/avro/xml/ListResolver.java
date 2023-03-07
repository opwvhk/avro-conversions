package opwvhk.avro.xml;

import java.util.ArrayList;
import java.util.List;

class ListResolver extends ValueResolver {
	private final ValueResolver resolver;

	public ListResolver(ValueResolver resolver) {
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
		((List<Object>)collector).add(value);
		return collector;
	}
}
