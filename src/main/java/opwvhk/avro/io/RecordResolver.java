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
    private static final Object NOT_SET = new Object();

    private final GenericData model;
    private final Schema recordSchema;
    private final Map<String, ValueResolver> resolversByName;
    private final Map<String, Schema.Field> fieldsByName;
    private final Set<String> arrayFields;

    /**
     * Create a records resolver for the given model and schema.
     *
     * @param model        a model to generate records with
     * @param recordSchema the record schema
     */
    public RecordResolver(GenericData model, Schema recordSchema) {
        this.model = model;
        this.recordSchema = recordSchema;
        resolversByName = new HashMap<>();
        fieldsByName = new HashMap<>();
        arrayFields = new HashSet<>();
    }

    /**
     * Add a resolver for an array field using the resolver for the array items.
     *
     * @param name     the name of the array property to resolve
     * @param field    the field in the schema that represents the property
     * @param resolver the resolver for the array items
     */
    public void addArrayResolver(String name, Schema.Field field, ValueResolver resolver) {
        addResolver(name, field, resolver);
        arrayFields.add(name);
    }

    /**
     * Add a resolver for a field.
     *
     * @param name     the name of the property to resolve
     * @param field    the field in the schema that represents the property
     * @param resolver the resolver for the field value
     */
    public void addResolver(String name, Schema.Field field, ValueResolver resolver) {
        resolversByName.put(name, resolver);
        fieldsByName.put(name, field);
    }

    @Override
    public ValueResolver resolve(String name) {
        return resolversByName.computeIfAbsent(name,
                super::resolve);
    }

    @Override
    public Object createCollector() {
        Object newRecord = model.newRecord(null, recordSchema);
        for (Schema.Field field : recordSchema.getFields()) {
            model.setField(newRecord, field.name(), field.pos(), NOT_SET);
        }

        return newRecord;
    }

    @Override
    public Object addProperty(Object record, String name, Object value) {
        Schema.Field field = fieldsByName.get(name);
        // If null, the field is unknown and should be ignored.
        if (field != null) {
            if (arrayFields.contains(name)) {
                Object maybeList = model.getField(record, field.name(), field.pos());
                if (maybeList == NOT_SET) {
                    maybeList = new ArrayList<>();
                    model.setField(record, field.name(), field.pos(), maybeList);
                }
                Collection<Object> list = (Collection<Object>) maybeList;
                list.add(value);
            } else {
                model.setField(record, field.name(), field.pos(), value);
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
            if (model.getField(collector, field.name(), field.pos()) == NOT_SET) {
                Object value = field.hasDefaultValue() ? model.getDefaultValue(field) : null; // Don't leak internal object; using the object will fail anyway
                model.setField(collector, field.name(), field.pos(), value);
            }
        }
        return collector;
    }
}
