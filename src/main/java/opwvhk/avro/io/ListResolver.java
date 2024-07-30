package opwvhk.avro.io;

import java.util.ArrayList;
import java.util.List;

/**
 * A resolver for list values.
 */
public class ListResolver extends ValueResolver {
	private final ValueResolver resolver;

	/**
	 * Create a list resolver using the resolver for items.
	 *
	 * @param resolver the resolver to use for list items
	 */
	public ListResolver(ValueResolver resolver) {
		this.resolver = resolver;
	}

	@Override
	public ValueResolver resolve(String name) {
		return resolver;
	}

	@Override
	public Object addContent(Object collector, String content) {
		Object value = resolver.complete(resolver.addContent(resolver.createCollector(), content));
		return addProperty(collector, "ignored", value);
	}

	@Override
	public Object createCollector() {
		return new ArrayList<>();
	}

	@Override
	public Object addProperty(Object collector, String name, Object value) {
		((List<Object>) collector).add(value);
		return collector;
	}
}
