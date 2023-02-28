package opwvhk.avro.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class RecordResolver
		extends ValueResolver {
	private final GenericData model;
	private final Schema recordSchema;
	private final Map<String, ValueResolver> resolversByName;
	private final Map<String, Integer> fieldPositionsByName;
	private final Set<String> arrayFields;

	public RecordResolver(GenericData model, Schema recordSchema) {
		this.model = model;
		this.recordSchema = recordSchema;
		resolversByName = new HashMap<>();
		fieldPositionsByName = new HashMap<>();
		arrayFields = new HashSet<>();
	}

	public void addArrayResolver(String name, int position, ValueResolver resolver) {
		addResolver(name, position, resolver);
		arrayFields.add(name);
	}

	public void addResolver(String name, int position, ValueResolver resolver) {
		resolversByName.put(name, resolver);
		fieldPositionsByName.put(name, position);
	}

	@Override
	public ValueResolver resolve(String name) {
		return resolversByName.computeIfAbsent(name,
				super::resolve);
	}

	@Override
	public Object createCollector() {
		Object record = model.newRecord(null, recordSchema);
		for (Schema.Field field : recordSchema.getFields()) {
			if (field.hasDefaultValue()) {
				Object defaultValue = model.getDefaultValue(field);
				Object copyOfPossibleCachedDefaultValue = model.deepCopy(field.schema(), defaultValue);
				model.setField(record, field.name(), field.pos(), copyOfPossibleCachedDefaultValue);
			}
		}
		return record;
	}

	@Override
	public Object addProperty(Object collector, String name, Object value) {
		Integer position = fieldPositionsByName.get(name);
		// If null, the field is unknown and should be ignored.
		if (position != null) {
			if (arrayFields.contains(name)) {
				Collection<Object> list = (Collection<Object>) model.getField(collector, name, position);
				if (list == null) {
					list = new ArrayList<>();
					model.setField(collector, name, position, list);
				}
				list.add(value);
			} else {
				model.setField(collector, name, position, value);
			}
		}
		return collector;
	}

	@Override
	public Object addContent(Object collector, String content) {
		ValueResolver valueResolver = resolve("value");
		Object valueCollector = valueResolver.createCollector();
		valueCollector = valueResolver.addContent(valueCollector, content);
		valueCollector = valueResolver.complete(valueCollector);
		return addProperty(collector, "value", valueCollector);
	}
}
