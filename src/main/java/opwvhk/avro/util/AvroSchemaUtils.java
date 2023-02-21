package opwvhk.avro.util;

import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.compiler.schema.SchemaVisitor;
import org.apache.avro.compiler.schema.SchemaVisitorAction;
import org.apache.avro.compiler.schema.Schemas;

import static java.util.stream.Stream.concat;
import static org.apache.avro.Schema.Type.RECORD;

public final class AvroSchemaUtils {

	/**
	 * Return an equivalent Avro schema, with all fields sorted by name.
	 *
	 * @param schema an Avro schema
	 * @return the equivalent Avro schema, but with sorted fields
	 */
	public static Schema sortFields(Schema schema) {
		Comparator<Schema.Field> comparator = Comparator.comparing(Schema.Field::name);

		IdentityHashMap<Schema, Schema> updatedSchemas = new IdentityHashMap<>();
		return Schemas.visit(schema, new SchemaVisitor<>() {
			@Override
			public SchemaVisitorAction visitTerminal(Schema terminal) {
				return SchemaVisitorAction.CONTINUE;
			}

			@Override
			public SchemaVisitorAction visitNonTerminal(Schema nonTerminal) {
				if (nonTerminal.getType() == RECORD) {
					// Records can be referenced before they're redefined!
					Schema newRecord = Schema.createRecord(nonTerminal.getName(), nonTerminal.getDoc(), nonTerminal.getNamespace(), nonTerminal.isError());
					newRecord.addAllProps(nonTerminal);
					updatedSchemas.put(nonTerminal, newRecord);
				}
				return SchemaVisitorAction.CONTINUE;
			}

			@Override
			public SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal) {
				switch (nonTerminal.getType()) {
					case RECORD -> {
						List<Schema.Field> sortedFieldList = nonTerminal.getFields().stream().sorted(comparator)
								// We recreate the fields because once added to a record they cannot be reused as-is
								// Also, the schema lookup is to ensure we're using updated, sorted types even if the fields of this record were sorted
								.map(f -> new Schema.Field(f, get(f.schema()))).toList();
						updatedSchemas.get(nonTerminal).setFields(sortedFieldList);
					}
					case ARRAY -> updatedSchemas.put(nonTerminal, Schema.createArray(get(nonTerminal.getElementType())));
					//case MAP -> updatedSchemas.put(nonTerminal, Schema.createMap(get(nonTerminal.getValueType())));
					default -> {  // nonTerminal.getType() == UNION
						List<Schema> newUnionTypes = nonTerminal.getTypes().stream().map(this::get).toList();
						updatedSchemas.put(nonTerminal, Schema.createUnion(newUnionTypes));
					}
				}
				return SchemaVisitorAction.CONTINUE;
			}

			private Schema get(Schema schema) {
				return updatedSchemas.getOrDefault(schema, schema);
			}

			@Override
			public Schema get() {
				return get(schema);
			}
		});
	}

	/**
	 * Lists all names in the Avro schema, as path from the schema root, combined its documentation (if any). Each entry in the result is a concatenation of 1
	 * or more names, separated by dots.
	 *
	 * @param schema an Avro schema
	 * @return all name paths in the schema
	 */
	public static String documentAsMarkdown(Schema schema) {
		return "| Field(path) | Type | Documentation |\n|-------------|------|---------------|\n" + describeSchema(schema)
				.map(entry -> "| %s | %s | %s |".formatted(entry.path(), entry.type(), entry.docForMDTableCell()))
				.collect(Collectors.joining("\n"));
	}

	private static Stream<Entry> describeSchema(Schema schema) {
		return describeSchema("", null, schema);
	}

	private static Stream<Entry> describeSchema(String path, String fieldDoc, Schema schema) {
		return switch (schema.getType()) {
			case RECORD -> {
				String pathPrefix = path.isEmpty() ? "" : path + ".";
				yield concat(
						Stream.of(entryForSchema(path, fieldDoc, schema)),
						schema.getFields().stream()
								.flatMap(field -> describeSchema(pathPrefix + field.name(), field.doc(), field.schema())));
			}
			case UNION -> {
				String newPath = schema.isNullable() ? path + "?" : path;
				yield schema.getTypes().stream()
						.filter(s -> s.getType() != Schema.Type.NULL)
						.flatMap(s -> describeSchema(newPath, fieldDoc, s));
			}
			case ARRAY -> describeSchema(path + "[]", fieldDoc, schema.getElementType());
			case MAP -> describeSchema(path + "()", fieldDoc, schema.getValueType());
			default -> Stream.of(entryForSchema(path, fieldDoc, schema));
		};
	}

	private static Entry entryForSchema(String path, String fieldDoc, Schema schema) {
		String type;
		LogicalType logicalType = schema.getLogicalType();
		if (logicalType == null) {
			type = schema.getType().getName();
		} else if (logicalType instanceof LogicalTypes.Decimal decimal) {
			type = "decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
		} else {
			type = logicalType.getName();
		}

		StringJoiner doc = new StringJoiner("\n");
		Optional.ofNullable(fieldDoc).ifPresent(doc::add);
		Optional.ofNullable(schema.getDoc()).map(s -> "Type: " + s).ifPresent(doc::add);

		return new Entry(path, type, doc.toString());
	}

	record Entry(String path, String type, String documentation) {
		/**
		 * Returns the documentation with some rudimentary escapes, ensuring display in a Markdown table cell works well.
		 *
		 * @return the documentation, escaped to be used in Markdown table cells
		 */
		public String docForMDTableCell() {
			return documentation.replace("<", "&lt;").replace("\n", "<br/>");
		}
	}

	private AvroSchemaUtils() {
		// Utility class: no need to instantiate.
	}
}
