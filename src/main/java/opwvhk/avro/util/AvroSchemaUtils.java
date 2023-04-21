package opwvhk.avro.util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.compiler.schema.SchemaVisitor;
import org.apache.avro.compiler.schema.SchemaVisitorAction;
import org.apache.avro.compiler.schema.Schemas;

import static java.util.stream.Stream.concat;

/**
 * Utility class to document Avro schemas.
 */
public final class AvroSchemaUtils {

    /**
     * <p>Lists all names in the Avro schema as rows in a Markdown table, as path from the schema root, combined its documentation (if any).</p>
     *
     * <p>Each entry in the result is a concatenation of 1 or more names, separated by dots.</p>
     *
     * <p>Note that the implementation is safe for recursive schemata, but recursive types are not marked. You won't recognize them unless they have unique
     * documentation.</p>
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
        IdentityHashMap<Schema, Schema> seen = new IdentityHashMap<>();
        return describeSchema("", null, schema, seen);
    }

    private static Stream<Entry> describeSchema(String path, String fieldDoc, Schema schema, IdentityHashMap<Schema, Schema> seen) {
        if (seen.put(schema, schema) != null) {
            return Stream.of(entryForSchema(path, fieldDoc, schema));
        }
        return switch (schema.getType()) {
            case RECORD -> {
                String pathPrefix = path.isEmpty() ? "" : path + ".";
                yield concat(
                        Stream.of(entryForSchema(path, fieldDoc, schema)),
                        schema.getFields().stream()
                                .flatMap(field -> describeSchema(pathPrefix + field.name(), field.doc(), field.schema(), seen)));
            }
            case UNION -> {
                String newPath = schema.isNullable() ? path + "?" : path;
                yield schema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .flatMap(s -> describeSchema(newPath, fieldDoc, s, seen));
            }
            case ARRAY -> describeSchema(path + "[]", fieldDoc, schema.getElementType(), seen);
            case MAP -> describeSchema(path + "()", fieldDoc, schema.getValueType(), seen);
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

    /**
     * Validate that the given schema has only unique names for all contains (sub)schemata and fields. Throws if the list of names and aliases of the named
     * (sub)schemata has duplicates, or if the list of names and aliases of any (sub)schema has duplicates.
     *
     * @param schema a schema whose names to validate for uniqueness
     */
    public static void requireUniqueNames(Schema schema) {
        Map<String, Set<String>> duplicateNames = Schemas.visit(schema, new SchemaVisitor<>() {
	        private final Map<String, Set<String>> seenNames = new LinkedHashMap<>();
	        private final Map<String, Set<String>> duplicateNames = new LinkedHashMap<>();

	        @Override
	        public SchemaVisitorAction visitTerminal(Schema terminal) {
		        if (terminal.getType() == Schema.Type.ENUM || terminal.getType() == Schema.Type.FIXED) {
			        checkNames(null, terminal.getFullName(), terminal.getAliases());
		        }
		        return SchemaVisitorAction.CONTINUE;
	        }

	        private void checkNames(String containerName, String fullName, Set<String> aliases) {
		        checkName(containerName, fullName);
		        for (String alias : aliases) {
			        checkName(containerName, alias);
		        }
	        }

	        private void checkName(String containerName, String name) {
		        if (!seenNames.computeIfAbsent(containerName, _key -> new LinkedHashSet<>()).add(name)) {
                    // Adding the name did not change the set of seen names for this container, so we've already seen it before.
			        duplicateNames.computeIfAbsent(containerName, _key -> new LinkedHashSet<>()).add(name);
		        }
	        }

	        @Override
	        public SchemaVisitorAction visitNonTerminal(Schema nonTerminal) {
		        if (nonTerminal.getType() == Schema.Type.RECORD) {
			        checkNames(null, nonTerminal.getFullName(), nonTerminal.getAliases());
			        for (Schema.Field field : nonTerminal.getFields()) {
				        checkNames(nonTerminal.getFullName(), field.name(), field.aliases());
			        }
		        }
		        return SchemaVisitorAction.CONTINUE;
	        }

	        @Override
	        public SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal) {
		        return SchemaVisitorAction.CONTINUE;
	        }

	        @Override
	        public Map<String, Set<String>> get() {
		        return duplicateNames;
	        }
        });
        if (!duplicateNames.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Not all names are unique.\n");
            Set<String> duplicateSchemaNames = Optional.ofNullable(duplicateNames.remove(null)).orElse(Collections.emptySet());
            StringJoiner joiner = new StringJoiner(", ", "Duplicate class names: ", "\n");
            joiner.setEmptyValue("");
            duplicateSchemaNames.forEach(joiner::add);
            errorMessage.append(joiner);
            for (String schemaName : duplicateNames.keySet()) {
                joiner = new StringJoiner(", ", "Duplicate field names for " + schemaName + ": ", "\n");
                duplicateNames.get(schemaName).forEach(joiner::add);
                errorMessage.append(joiner);
            }
            throw new IllegalArgumentException(errorMessage.toString());
        }
    }

	/**
     * Determine the non-nullable version of a union schema. Assumes that the schema is not the null schema, and that unions only contain a single non-null
     * type. Yields undefined results otherwise.
     *
     * @param readSchema a schema
     * @return the non-nullable version of the schema
     */
    public static Schema nonNullableSchemaOf(Schema readSchema) {
        if (readSchema.isUnion()) {
            return readSchema.getTypes().stream().filter(s -> !s.isNullable()).findAny().orElseThrow();
        } else {
            return readSchema;
        }
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
