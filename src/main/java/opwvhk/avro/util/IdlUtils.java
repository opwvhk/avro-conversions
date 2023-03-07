package opwvhk.avro.util;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.json.JsonWriteContext;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import static java.util.Objects.requireNonNull;

public final class IdlUtils {
	static final JsonFactory SCHEMA_FACTORY;
	private static final Function<Field, JsonNode> DEFAULT_VALUE;

	static {
		SCHEMA_FACTORY = getFieldValue(getField(Schema.class, "FACTORY"), null);

		java.lang.reflect.Field defaultValueField = getField(Field.class, "defaultValue");
		DEFAULT_VALUE = field -> getFieldValue(defaultValueField, field);
	}

	static java.lang.reflect.Field getField(Class<?> type, String name) {
		try {
			java.lang.reflect.Field field = type.getDeclaredField(name);
			field.setAccessible(true);
			return field;
		} catch (NoSuchFieldException e) {
			throw new IllegalStateException("Programmer error", e);
		}
	}

	static <T> T getFieldValue(java.lang.reflect.Field field, Object owner) {
		try {
			return (T) field.get(owner);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Programmer error", e);
		}
	}

	static java.lang.reflect.Method getMethod(Class<?> type, String name, Class<?>... parameterTypes) {
		try {
			java.lang.reflect.Method method = type.getDeclaredMethod(name, parameterTypes);
			method.setAccessible(true);
			return method;
		} catch (NoSuchMethodException e) {
			throw new IllegalStateException("Programmer error", e);
		}
	}

	static <T> T invokeMethod(java.lang.reflect.Method method, Object owner, Object... parameters) {
		try {
			return (T) method.invoke(owner, parameters);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new IllegalStateException("Programmer error", e);
		}
	}

	private static final java.lang.reflect.Method RESET_METHOD = getMethod(JsonWriteContext.class, "reset", Integer.TYPE);

	private static void resetContext(final JsonStreamContext context) {
		invokeMethod(RESET_METHOD, context, 0);
	}

	private IdlUtils() {
		// Utility class: do not instantiate.
	}

	public static void writeIdlProtocol(final String protocolNameSpace, final String protocolName, final Writer writer, final Schema... schemas)
			throws IOException {
		try (JsonGenerator jsonGen = SCHEMA_FACTORY.createGenerator(writer)) {
			writer.append("@namespace(\"").append(requireNonNull(protocolNameSpace)).append("\")\n");
			writer.append("protocol ").append(requireNonNull(protocolName)).append(" {\n");

			Set<String> alreadyDeclared = new HashSet<>(4);
			Set<Schema> toDeclare = new LinkedHashSet<>(Arrays.asList(schemas));
			while (!toDeclare.isEmpty()) {
				Iterator<Schema> iterator = toDeclare.iterator();
				Schema schema = iterator.next();
				iterator.remove();
				writeSchema(schema, writer, jsonGen, protocolNameSpace, alreadyDeclared, toDeclare);
				writer.append('\n');
			}
		}
		writer.append("}\n");
	}

	private static void writeSchema(Schema schema, final Writer writer, JsonGenerator jsonGen, final String protocolNameSpace,
	                                final Set<String> alreadyDeclared, final Set<Schema> toDeclare) throws IOException {
		Schema.Type type = schema.getType();
		writeSchemaAttributes(schema, writer, jsonGen);
		String namespace = schema.getNamespace(); // Fails for unnamed schema types (i.e., other than record, enum & fixed)
		if (!Objects.equals(namespace, protocolNameSpace)) {
			requireNonNull(namespace, "IDL does not allow referencing the default namespace from within another namespace");
			writer.append("    @namespace(\"").append(namespace).append("\")\n");
		}
		Set<String> schemaAliases = schema.getAliases();
		if (!schemaAliases.isEmpty()) {
			writer.append("    @aliases(");
			toJson(schemaAliases, jsonGen);
			jsonGen.flush();
			resetContext(jsonGen.getOutputContext());
			writer.append(")\n");
		}
		if (type == Schema.Type.RECORD) {
			writer.append("    record ").append(schema.getName()).append(" {\n");
			alreadyDeclared.add(schema.getFullName());
			for (Field field : schema.getFields()) {
				String fDoc = field.doc();
				if (fDoc != null) {
					writer.append("        /** ").append(fDoc.replace("\n", "\n        ")).append(" */\n");
				}
				writer.append("        ");
				writeFieldSchema(field.schema(), writer, jsonGen, alreadyDeclared, toDeclare, schema.getNamespace());
				writer.append(' ');
				Set<String> fieldAliases = field.aliases();
				if (!fieldAliases.isEmpty()) {
					writer.append("@aliases(");
					toJson(fieldAliases, jsonGen);
					jsonGen.flush();
					resetContext(jsonGen.getOutputContext());
					writer.append(") ");
				}
				Field.Order order = field.order();
				if (order != Field.Order.ASCENDING) {
					writer.append("@order(\"").append(order.name()).append("\") ");
				}
				writeJsonProperties(field, writer, jsonGen, false);
				writer.append(' ');
				writer.append(field.name());
				JsonNode defaultValue = DEFAULT_VALUE.apply(field);
				if (defaultValue != null) {
					writer.append(" = ");
					toJson(field.defaultVal(), jsonGen);
					jsonGen.flush();
					resetContext(jsonGen.getOutputContext());
				}
				writer.append(";\n");
			}
			writer.append("}\n");
		} else if (type == Schema.Type.ENUM) {
			writer.append("    enum ").append(schema.getName()).append(" {");
			alreadyDeclared.add(schema.getFullName());
			Iterator<String> i = schema.getEnumSymbols().iterator();
			if (i.hasNext()) {
				writer.append(i.next());
				while (i.hasNext()) {
					writer.append(',');
					writer.append(i.next());
				}
			} else {
				throw new IllegalStateException("Enum schema must have at least a symbol " + schema);
			}
			writer.append("}\n");
		} else /* (type == Schema.Type.FIXED) */ {
			writer.append("    fixed ").append(schema.getName()).append('(').append(Integer.toString(schema.getFixedSize())).append(");\n");
			alreadyDeclared.add(schema.getFullName());
		}
	}

	private static void writeFieldSchema(final Schema schema, final Writer writer, final JsonGenerator jsonGen, final Set<String> alreadyDeclared,
	                                     final Set<Schema> toDeclare, final String recordNameSpace) throws IOException {
		Schema.Type type = schema.getType();
		if (type == Schema.Type.RECORD || type == Schema.Type.ENUM || type == Schema.Type.FIXED) {
			if (Objects.equals(recordNameSpace, schema.getNamespace())) {
				writer.append(schema.getName());
			} else {
				writer.append(schema.getFullName());
			}
			if (!alreadyDeclared.contains(schema.getFullName())) {
				toDeclare.add(schema);
			}
		} else if (type == Schema.Type.ARRAY) {
			writeJsonProperties(schema, writer, jsonGen, false);
			writer.append("array<");
			writeFieldSchema(schema.getElementType(), writer, jsonGen, alreadyDeclared, toDeclare, recordNameSpace);
			writer.append('>');
		} else if (type == Schema.Type.MAP) {
			writeJsonProperties(schema, writer, jsonGen, false);
			writer.append("map<");
			writeFieldSchema(schema.getValueType(), writer, jsonGen, alreadyDeclared, toDeclare, recordNameSpace);
			writer.append('>');
		} else if (type == Schema.Type.UNION) {
			writeJsonProperties(schema, writer, jsonGen, false);
			writer.append("union {");
			List<Schema> types = schema.getTypes();
			Iterator<Schema> iterator = types.iterator();
			if (iterator.hasNext()) {
				writeFieldSchema(iterator.next(), writer, jsonGen, alreadyDeclared, toDeclare, recordNameSpace);
				while (iterator.hasNext()) {
					writer.append(',');
					writeFieldSchema(iterator.next(), writer, jsonGen, alreadyDeclared, toDeclare, recordNameSpace);
				}
			} else {
				throw new IllegalStateException("Union schemas must have member types " + schema);
			}
			writer.append('}');
		} else {
			final Set<String> propertiesToSkip;
			final String typeName;
			if (schema.getLogicalType() == null) {
				propertiesToSkip = Collections.emptySet();
				typeName = schema.getName();
			} else {
				String logicalName = schema.getLogicalType().getName();
				switch (logicalName) {
					case "date", "time-millis", "timestamp-millis" -> {
						propertiesToSkip = Collections.singleton("logicalType");
						typeName = logicalName.replace("-millis", "_ms");
					}
					case "decimal" -> {
						propertiesToSkip = Set.of("logicalType", "precision", "scale");
						LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) schema.getLogicalType();
						typeName = String.format("decimal(%d,%d)", decimal.getPrecision(), decimal.getScale());
					}
					default -> {
						propertiesToSkip = Collections.emptySet();
						typeName = schema.getName();
					}
				}
			}
			writeJsonProperties(schema, propertiesToSkip, writer, jsonGen, false);
			writer.append(typeName);
		}
	}

	private static void writeSchemaAttributes(final Schema schema, final Writer writer, final JsonGenerator jsonGen) throws IOException {
		String doc = schema.getDoc();
		if (doc != null) {
			writer.append("    /** ").append(doc.replace("\n", "\n    ")).append(" */\n");
		}
		writeJsonProperties(schema, writer, jsonGen, true);
	}

	private static void writeJsonProperties(final JsonProperties props, final Writer writer, final JsonGenerator jsonGen, final boolean crBetween)
			throws IOException {
		writeJsonProperties(props, Collections.emptySet(), writer, jsonGen, crBetween);
	}

	private static void writeJsonProperties(final JsonProperties props, final Set<String> propertiesToSkip, final Writer writer, final JsonGenerator jsonGen,
	                                        final boolean crBetween) throws IOException {
		Map<String, Object> objectProps = props.getObjectProps();
		for (Map.Entry<String, Object> entry : objectProps.entrySet()) {
			if (propertiesToSkip.contains(entry.getKey())) {
				continue;
			}
			writer.append('@').append(entry.getKey()).append('(');
			toJson(entry.getValue(), jsonGen);
			jsonGen.flush();
			resetContext(jsonGen.getOutputContext());
			writer.append(')');
			if (crBetween) {
				writer.append('\n');
			} else {
				writer.append(' ');
			}
		}
	}

	static void toJson(final Object datum, final JsonGenerator generator) throws IOException {
		if (datum == JsonProperties.NULL_VALUE) { // null
			generator.writeNull();
		} else if (datum instanceof Map) { // record, map
			generator.writeStartObject();
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) datum).entrySet()) {
				generator.writeFieldName(entry.getKey().toString());
				toJson(entry.getValue(), generator);
			}
			generator.writeEndObject();
		} else if (datum instanceof Collection) { // array
			generator.writeStartArray();
			for (Object element : (Collection<?>) datum) {
				toJson(element, generator);
			}
			generator.writeEndArray();
		} else if (datum instanceof byte[]) { // bytes, fixed
			generator.writeString(new String((byte[]) datum, StandardCharsets.ISO_8859_1));
		} else if (datum instanceof CharSequence || datum instanceof Enum<?>) { // string, enum
			generator.writeString(datum.toString());
		} else if (datum instanceof Double) { // double
			generator.writeNumber((Double) datum);
		} else if (datum instanceof Float) { // float
			generator.writeNumber((Float) datum);
		} else if (datum instanceof Long) { // long
			generator.writeNumber((Long) datum);
		} else if (datum instanceof Integer) { // int
			generator.writeNumber((Integer) datum);
		} else if (datum instanceof Boolean) { // boolean
			generator.writeBoolean((Boolean) datum);
		} else {
			throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
		}
	}
}
