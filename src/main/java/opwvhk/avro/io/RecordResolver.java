package opwvhk.avro.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/**
 * Create a record resolver for Avro records.
 */
public class RecordResolver
		extends ValueResolver {
	private final GenericData model;
	private final Schema recordSchema;
	private final Map<String, ValueResolver> resolversByName;
	private final Map<String, Integer> fieldPositionsByName;
	private final Set<String> arrayFields;

	/**
	 * Create a records resolver for the given model and schema.
	 *
	 * @param model a model to generate records with
	 * @param recordSchema the record schema
	 */
	public RecordResolver(GenericData model, Schema recordSchema) {
		this.model = model;
		this.recordSchema = recordSchema;
		resolversByName = new HashMap<>();
		fieldPositionsByName = new HashMap<>();
		arrayFields = new HashSet<>();
	}

	/**
	 * Add a resolver for an array field using the resolver for the array items.
	 *
	 * @param name the name of the array property to resolve
	 * @param position the position of the array field in the schema
	 * @param resolver the resolver for the array items
	 */
	public void addArrayResolver(String name, int position, ValueResolver resolver) {
		addResolver(name, position, resolver);
		arrayFields.add(name);
	}

	/**
	 * Add a resolver for a field.
	 *
	 * @param name the name of the property to resolve
	 * @param position the position of the field in the schema
	 * @param resolver the resolver for the field value
	 */
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
		return model.newRecord(null, recordSchema);
	}

	@Override
	public Object addProperty(Object record, String name, Object value) {
		Integer position = fieldPositionsByName.get(name);
		// If null, the field is unknown and should be ignored.
		if (position != null) {
			if (arrayFields.contains(name)) {
				Collection<Object> list = (Collection<Object>) model.getField(record, name, position);
				if (list == null) {
					list = new ArrayList<>();
					model.setField(record, name, position, list);
				}
				list.add(value);
			} else {
				model.setField(record, name, position, value);
			}
		}
		return record;
	}

	@Override
	public Object addContent(Object record, String content) {
		ValueResolver valueResolver = resolve("value");
		Object value = valueResolver.complete(valueResolver.addContent(valueResolver.createCollector(), content));
		return addProperty(record, "value", value);
	}

	@Override
	public Object complete(Object collector) {
		// Fill in default values for fields that have not been set.
		for (Schema.Field field : recordSchema.getFields()) {
			if (field.hasDefaultValue() && model.getField(collector, field.name(), field.pos()) == null) {
				model.setField(collector, field.name(), field.pos(), model.getDefaultValue(field));
			}
		}
		return collector;
	}

}
