package opwvhk.avro.util;

import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Stream;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import static java.util.stream.Stream.concat;

/**
 * Utility class to document Avro schemas.
 */
public final class AvroSchemaUtils {

	/**
	 * Lists all names in the Avro schema as rows in a Markdown table, as path from the schema root, combined its documentation (if any). Each entry in the
	 * result is a concatenation of 1 or more names, separated by dots.
	 *
	 * @param schema an Avro schema
	 * @return all name paths in the schema
	 */
	public static String documentAsMarkdown(Schema schema) {
		StringBuilder buffer = new StringBuilder();
		documentAsMarkdown(schema, buffer);
		return buffer.toString();
	}

	/**
	 * Lists all names in the Avro schema as rows in a Markdown table, as path from the schema root, combined its documentation (if any). Each entry in the
	 * result is a concatenation of 1 or more names, separated by dots.
	 *
	 * @param schema an Avro schema
	 * @param buffer the buffer to write the result to
	 */
	public static void documentAsMarkdown(Schema schema, StringBuilder buffer) {
		buffer.append(Entry.TABLE_HEADER);
		describeSchema(schema).forEach(buffer::append);
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
		private static final String TABLE_HEADER = "| Field(path) | Type | Documentation |\n|-------------|------|---------------|\n";
		private static final String TABLE_ENTRY_FORMAT = "| %s | %s | %s |\n";

		@Override
		public String toString() {
			// The path and type are either validated or under our control. But documentation needs escaping.
			// Also, documentation should show newlines, which is done in tables in Markdown using <br/> tags.
			String docForMDTableCell = this.documentation().replace("<", "&lt;").replace("\n", "<br/>");
			return TABLE_ENTRY_FORMAT.formatted(path(), type(), docForMDTableCell);
		}
	}

	private AvroSchemaUtils() {
		// Utility class: no need to instantiate.
	}
}
