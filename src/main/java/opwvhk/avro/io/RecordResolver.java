package opwvhk.avro.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class RecordResolver
		extends ValueResolver {
	private final GenericData model;
	private final Schema recordSchema;
	private final Map<String, ValueResolver> resolversByName;
	private final Map<String, Integer> fieldPositionsByName;

	public RecordResolver(GenericData model, Schema recordSchema) {
		this.model = model;
		this.recordSchema = recordSchema;
		resolversByName = new HashMap<>();
		fieldPositionsByName = new HashMap<>();
	}

	protected void addResolver(String name, int position, ValueResolver resolver) {
		resolversByName.put(name, resolver);
		fieldPositionsByName.put(name, position);
	}

	@Override
	public ValueResolver resolve(String name) {
		return resolversByName.computeIfAbsent(name, super::resolve);
	}

	@Override
	public Object createCollector() {
		return model.newRecord(null, recordSchema);
	}

	@Override
	public Object addProperty(Object collector, String name, Object value) {
		Integer position = fieldPositionsByName.get(name);
		if (position != null) {
			// If null, the field is unknown and should be ignored.
			model.setField(collector, name, position, value);
		}
		return collector;
	}

	@Override
	public Object addContent(Object collector, String content) {
		return addProperty(collector, "value", content);
	}
}
