package opwvhk.avro;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import opwvhk.avro.util.AvroSchemaUtils;
import opwvhk.avro.xml.XsdAnalyzer;
import org.apache.avro.Schema;

import static java.util.Objects.requireNonNull;

public class SchemaManipulator {
	private Schema initialSchema;
	private boolean sortFields;
	private boolean renameWithAliases;
	private StringBuilder markdownBuffer;
	List<SchemaRenamer> schemaRenamerList;
	List<FieldRenamer> fieldRenamerList;
	List<UnwrapTest> unwrapTests;

	private SchemaManipulator(Schema initialSchema) {
		reset(initialSchema);
	}

	private void reset(Schema initialSchema) {
		this.initialSchema = requireNonNull(initialSchema);
		sortFields = false;
		renameWithAliases = true;
		markdownBuffer = null;
		schemaRenamerList = new ArrayList<>();
		fieldRenamerList = new ArrayList<>();
		unwrapTests = new ArrayList<>();
	}

	public static SchemaManipulator startFromAvro(String avroSchemaAsString) {
		Schema schema = new Schema.Parser().parse(avroSchemaAsString);
		return new SchemaManipulator(schema);
	}

	public static SchemaManipulator startFromAvro(URL schemaLocation) throws IOException {
		try (InputStream inputStream = schemaLocation.openStream()) {
			Schema schema = new Schema.Parser().parse(inputStream);
			return new SchemaManipulator(schema);
		}
	}

	public static SchemaManipulator startFromXsd(URL schemaLocation, String rootElementName) throws IOException {
		XsdAnalyzer analyzer = new XsdAnalyzer(schemaLocation);
		Schema schema = analyzer.schemaOf(rootElementName);
		return new SchemaManipulator(schema);
	}

	public Schema finish() {
		Schema result = applySchemaChanges(initialSchema);

		if (markdownBuffer != null) {
			AvroSchemaUtils.documentAsMarkdown(result, markdownBuffer);
		}

		reset(result);

		return result;
	}

	private Schema applySchemaChanges(Schema schema) {
		IdentityHashMap<Schema, Schema> changedSchemasByOriginal = new IdentityHashMap<>();
		return applySchemaChanges(changedSchemasByOriginal, "", schema);
	}

	private Schema applySchemaChanges(IdentityHashMap<Schema, Schema> changedSchemasByOriginal, String path, Schema schema) {
		Schema existingResult = changedSchemasByOriginal.get(schema);
		if (existingResult != null) {
			return existingResult;
		}
		Schema newSchema = switch (schema.getType()) {
			case ARRAY -> Schema.createArray(applySchemaChanges(changedSchemasByOriginal, path, schema.getElementType()));
			case MAP -> Schema.createMap(applySchemaChanges(changedSchemasByOriginal, path, schema.getValueType()));
			case UNION -> {
				List<Schema> unionTypes = new ArrayList<>();
				schema.getTypes().forEach(s -> {
					Schema newUnionType = applySchemaChanges(changedSchemasByOriginal, path, s);
					unionTypes.add(newUnionType);
				});
				yield Schema.createUnion(unionTypes);
			}
			case RECORD -> {
				String newSchemaName = newSchemaName(path, schema);
				Schema newRecord;
				if (newSchemaName != null) {
					newRecord = Schema.createRecord(newSchemaName, schema.getDoc(), null, schema.isError());
					if (renameWithAliases) {
						newRecord.addAlias(schema.getFullName());
					}
				} else {
					newRecord = Schema.createRecord(schema.getFullName(), schema.getDoc(), null, schema.isError());
				}
				schema.getAliases().forEach(newRecord::addAlias);
				changedSchemasByOriginal.put(schema, newRecord); // Do this early to stop recursion

				List<Schema.Field> newFields = new ArrayList<>(schema.getFields().size());
				for (Schema.Field field : schema.getFields()) {
					String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
					String newFieldName = newFieldName(fieldPath, schema, field);

					Schema newFieldSchema = field.schema();
					String newDoc = field.doc();
					Object newDefaultValue = field.defaultVal();
					Schema.Field.Order newOrder = field.order();
					Schema.Field wrappedArrayField = onlyRecordFieldIfArray(newFieldSchema);
					if (wrappedArrayField != null && unwrapTests.stream().anyMatch(uwt -> uwt.test(fieldPath, schema, field, wrappedArrayField))) {
						newFieldSchema = wrappedArrayField.schema();
						newDoc = wrappedArrayField.doc();
						newDefaultValue = wrappedArrayField.defaultVal();
						newOrder = wrappedArrayField.order();
					}

					newFieldSchema = applySchemaChanges(changedSchemasByOriginal, fieldPath, newFieldSchema);
					Schema.Field newField;
					if (newFieldName != null) {
						newField = new Schema.Field(newFieldName, newFieldSchema, newDoc, newDefaultValue, newOrder);
						if (renameWithAliases) {
							newField.addAlias(field.name());
						}
					} else {
						newField = new Schema.Field(field.name(), newFieldSchema, newDoc, newDefaultValue, newOrder);
					}
					field.aliases().forEach(newField::addAlias);
					newField.addAllProps(field);
					newFields.add(newField);
				}
				if (sortFields) {
					newFields.sort(Comparator.comparing(Schema.Field::name));
				}
				newRecord.setFields(newFields);
				yield newRecord;
			}
			case ENUM -> {
				String newSchemaName = newSchemaName(path, schema);
				if (newSchemaName == null) {
					yield schema;
				} else {
					yield Schema.createEnum(newSchemaName, schema.getDoc(), null, schema.getEnumSymbols(), schema.getEnumDefault());
				}
			}
			case FIXED -> {
				String newSchemaName = newSchemaName(path, schema);
				if (newSchemaName == null) {
					yield schema;
				} else {
					yield Schema.createFixed(newSchemaName, schema.getDoc(), null, schema.getFixedSize());
				}
			}
			default -> schema;
		};
		newSchema.addAllProps(schema);
		changedSchemasByOriginal.put(schema, newSchema); // Effective no-op for records, but that's harmless
		return newSchema;
	}

	private String newSchemaName(String path, Schema schema) {
		return schemaRenamerList.stream()
				.map(renamer -> renamer.newSchemaName(path, schema))
				.filter(Objects::nonNull)
				.findAny()
				.orElse(null);
	}

	private String newFieldName(String path, Schema schemaWithField, Schema.Field field) {
		return fieldRenamerList.stream()
				.map(renamer -> renamer.newFieldName(path, schemaWithField, field))
				.filter(Objects::nonNull)
				.findAny()
				.orElse(null);
	}

	private static Schema.Field onlyRecordFieldIfArray(Schema schema) {
		if (schema.isUnion() && schema.isNullable() && schema.getTypes().size() == 2) {
			schema = schema.getTypes().stream().filter(s -> !s.isNullable()).findFirst().orElseThrow();
		}
		if (schema.getType() == Schema.Type.RECORD && schema.getFields().size() == 1) {
			Schema.Field onlyField = schema.getFields().get(0);
			if (onlyField.schema().getType() == Schema.Type.ARRAY) {
				return onlyField;
			}
		}
		return null;
	}

	public SchemaManipulator sortFields() {
		sortFields = true;
		return this;
	}

	public SchemaManipulator renameWithoutAliases() {
		renameWithAliases = false;
		return this;
	}

	public SchemaManipulator renameSchema(String schemaName, String newSchemaName) {
		schemaRenamerList.add((pathToField, fieldSchema) ->
				isOneOf(schemaName, fieldSchema.getFullName(), fieldSchema.getAliases()) ? newSchemaName : null);
		return this;
	}

	private static boolean isOneOf(String test, String first, Set<String> others) {
		return first.equals(test) || others.contains(test);
	}

	public SchemaManipulator renameSchemaAtPath(String newSchemaName, String... pathToFieldWithSchemaToRename) {
		String pathToMatch = String.join(".", pathToFieldWithSchemaToRename);
		schemaRenamerList.add((pathToField, fieldSchema) ->
				pathToMatch.equals(pathToField) ? newSchemaName : null);
		return this;
	}

	public SchemaManipulator renameField(String schemaName, String fieldName, String newFieldName) {
		fieldRenamerList.add((pathToField, schemaWithField, field) ->
				isOneOf(schemaName, schemaWithField.getFullName(), schemaWithField.getAliases()) &&
				isOneOf(fieldName, field.name(), field.aliases()) ? newFieldName : null);
		return this;
	}

	public SchemaManipulator renameFieldAtPath(String newFieldName, String... pathToFieldToRename) {
		String pathToMatch = String.join(".", pathToFieldToRename);
		fieldRenamerList.add((pathToField, schemaWithField, fieldSchema) ->
				pathToMatch.equals(pathToField) ? newFieldName : null);
		return this;
	}

	public SchemaManipulator unwrapArrays(int ignoredMaxSuffixLength) {
		unwrapTests.add((path, schema, wrapping, wrapped) -> {
			String wrappingName = wrapping.name();
			String wrappedName = wrapped.name();
			int prefixLength = Math.max(0, Math.min(wrappingName.length(), wrappedName.length()) - ignoredMaxSuffixLength);
			return wrappingName.substring(0, prefixLength).equals(wrappedName.substring(0, prefixLength));
		});
		return this;
	}

	public SchemaManipulator unwrapArray(String schemaName, String wrappingField) {
		unwrapTests.add((path, schema, wrapping, wrapped) ->
				isOneOf(schemaName, schema.getFullName(), schema.getAliases()) &&
				isOneOf(wrappingField, wrapping.name(), wrapping.aliases())
		);
		return this;
	}

	public SchemaManipulator unwrapArrayAtPath(String... pathToWrappingField) {
		String pathToMatch = String.join(".", pathToWrappingField);
		unwrapTests.add((p, s, wr, wd) -> p.equals(pathToMatch));
		return this;
	}

	public SchemaManipulator alsoDocumentAsMarkdownTable(StringBuilder buffer) {
		markdownBuffer = buffer;
		return this;
	}

	public String asMarkdownTable() {
		StringBuilder buffer = new StringBuilder();
		alsoDocumentAsMarkdownTable(buffer).finish();
		return buffer.toString();
	}

	private interface SchemaRenamer {
		String newSchemaName(String pathToField, Schema fieldSchema);
	}

	private interface FieldRenamer {
		String newFieldName(String pathToField, Schema schemaWithField, Schema.Field field);
	}

	private interface UnwrapTest {
		boolean test(String pathToWrappingField, Schema schemaWithWrappingField, Schema.Field wrappingField, Schema.Field wrappedField);
	}
}
