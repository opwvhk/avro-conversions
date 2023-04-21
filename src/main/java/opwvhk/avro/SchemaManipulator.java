package opwvhk.avro;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import net.jimblackler.jsonschemafriend.GenerationException;
import opwvhk.avro.json.SchemaAnalyzer;
import opwvhk.avro.util.AvroSchemaUtils;
import opwvhk.avro.xml.XsdAnalyzer;
import org.apache.avro.Schema;

import static java.util.Objects.requireNonNull;

/**
 * Starting point for schema manipulation and documentation. Typical use case is to read an XSD, rename fields and schemata where needed, unwrap nested arrays,
 * and generate and document the resulting schema.
 */
public class SchemaManipulator {
	private Schema initialSchema;
	private boolean sortFields;
	private boolean renameWithAliases;
	private StringBuilder markdownBuffer;
	private List<SchemaRenamer> schemaRenamerList;
	private List<FieldRenamer> fieldRenamerList;
	private List<UnwrapTest> unwrapTests;

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

	/**
	 * Create a schema manipulator from an Avro schema as String.
	 *
	 * @param avroSchemaAsString an Avro schema (content of a {@code .avsc} file)
	 * @return a {@code SchemaManipulator}
	 */
	public static SchemaManipulator startFromAvro(String avroSchemaAsString) {
		Schema schema = new Schema.Parser().parse(avroSchemaAsString);
		return new SchemaManipulator(schema);
	}

	/**
	 * Create a schema manipulator from an Avro schema.
	 *
	 * @param schemaLocation the location of an Avro schema ({@code .avsc} file)
	 * @return a {@code SchemaManipulator}
	 */
	public static SchemaManipulator startFromAvro(URL schemaLocation) throws IOException {
		try (InputStream inputStream = schemaLocation.openStream()) {
			Schema schema = new Schema.Parser().parse(inputStream);
			return new SchemaManipulator(schema);
		}
	}

	/**
	 * Create a schema manipulator from an XML Schema Definition (XSD). The location of the main {@code .xsd} file is provided, both to provide the XSD content,
	 * as to provide a way to locate imported/included {@code .xsd} files.
	 *
	 * @param schemaLocation the location of the main {@code .xsd} file (it may include/import other {@code .xsd} files)
	 * @return a {@code SchemaManipulator}
	 */
	public static SchemaManipulator startFromXsd(URL schemaLocation, String rootElementName) throws IOException {
		XsdAnalyzer analyzer = new XsdAnalyzer(schemaLocation);
		Schema schema = analyzer.schemaOf(rootElementName);
		return new SchemaManipulator(schema);
	}

	/**
	 * Create a schema manipulator from a JSON Schema Definition.
	 *
	 * @param schemaLocation the location of the JSON schema (it may reference other JSON schemas)
	 * @return a {@code SchemaManipulator}
	 */
	public static SchemaManipulator startFromJsonSchema(URL schemaLocation) throws URISyntaxException, GenerationException {
		SchemaAnalyzer analyzer = new SchemaAnalyzer();
		Schema schema = analyzer.parseJsonSchema(schemaLocation.toURI());
		return new SchemaManipulator(schema);
	}

	/**
	 * <p>Complete the schema manipulation, write the documentation (if requested), and return the result.</p>
	 *
	 * <p>The {@code SchemaManipulator} remains available after calling this method, as if it were created with the resulting schema.</p>
	 *
	 * @return the resulting schema
	 */
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

	/**
	 * Return the resulting schema with sorted fields.
	 *
	 * @return this {@code SchemaManipulator}
	 */
	public SchemaManipulator sortFields() {
		sortFields = true;
		return this;
	}

	/**
	 * When renaming a field/schema, do not add the old name as alias (the default is to add aliases).
	 *
	 * @return this {@code SchemaManipulator}
	 * @see #renameSchema(String, String)
	 * @see #renameField(String, String, String)
	 * @see #renameSchemaAtPath(String, String...)
	 * @see #renameFieldAtPath(String, String...)
	 */
	public SchemaManipulator renameWithoutAliases() {
		renameWithAliases = false;
		return this;
	}

	/**
	 * Rename the specified schema.
	 *
	 * @param schemaName    the current name of the schema to rename
	 * @param newSchemaName the new name for the schema
	 * @return this {@code SchemaManipulator}
	 * @see #renameWithoutAliases()
	 * @see #renameSchemaAtPath(String, String...)
	 */
	public SchemaManipulator renameSchema(String schemaName, String newSchemaName) {
		schemaRenamerList.add((pathToField, fieldSchema) ->
				isOneOf(schemaName, fieldSchema.getFullName(), fieldSchema.getAliases()) ? newSchemaName : null);
		return this;
	}

	private static boolean isOneOf(String test, String first, Set<String> others) {
		return first.equals(test) || others.contains(test);
	}

	/**
	 * Rename the schema of the field at the specified path. To rename the main schema, provide an empty (no) path.
	 *
	 * @param newSchemaName                 the new name for the schema
	 * @param pathToFieldWithSchemaToRename the path to the field whose schema to rename
	 * @return this {@code SchemaManipulator}
	 * @see #renameWithoutAliases()
	 * @see #renameSchema(String, String)
	 */
	public SchemaManipulator renameSchemaAtPath(String newSchemaName, String... pathToFieldWithSchemaToRename) {
		String pathToMatch = String.join(".", pathToFieldWithSchemaToRename);
		schemaRenamerList.add((pathToField, fieldSchema) ->
				pathToMatch.equals(pathToField) ? newSchemaName : null);
		return this;
	}

	/**
	 * Rename the specified field in the (named) schema.
	 *
	 * @param schemaName   the name of the schema with the field to rename
	 * @param fieldName    the current name of the field to rename
	 * @param newFieldName the new name for the field
	 * @return this {@code SchemaManipulator}
	 * @see #renameWithoutAliases()
	 * @see #renameFieldAtPath(String, String...)
	 */
	public SchemaManipulator renameField(String schemaName, String fieldName, String newFieldName) {
		fieldRenamerList.add((pathToField, schemaWithField, field) ->
				isOneOf(schemaName, schemaWithField.getFullName(), schemaWithField.getAliases()) &&
				isOneOf(fieldName, field.name(), field.aliases()) ? newFieldName : null);
		return this;
	}

	/**
	 * Rename the field at the specified path.
	 *
	 * @param newFieldName        the new name for the field
	 * @param pathToFieldToRename the path to the field to rename
	 * @return this {@code SchemaManipulator}
	 * @see #renameWithoutAliases()
	 * @see #renameField(String, String, String)
	 */
	public SchemaManipulator renameFieldAtPath(String newFieldName, String... pathToFieldToRename) {
		String pathToMatch = String.join(".", pathToFieldToRename);
		fieldRenamerList.add((pathToField, schemaWithField, fieldSchema) ->
				pathToMatch.equals(pathToField) ? newFieldName : null);
		return this;
	}

	/**
	 * <p>Unwrap all arrays whose field names (except up to the last {@code ignoredMaxSuffixLength} characters) are equal.</p>
	 *
	 * <p>Wrapped arrays are an XML construct. They result in array fields without siblings in a record field (optionally in a union with null). In Avro,
	 * Parquet, and in fact most/all other formats, they are both not needed and unwanted. This method unwraps them based on the names of the wrapped and
	 * wrapping fields.</p>
	 *
	 * <p>When unwrapping, wrapped field will replace the wrapping field using the name of the wrapping field. As this is not a renaming action, no alias will
	 * be added.</p>
	 *
	 * @param ignoredMaxSuffixLength the length of the field suffix to ignore
	 * @return this {@code SchemaManipulator}
	 * @see #unwrapArray(String, String)
	 * @see #unwrapArrayAtPath(String...)
	 */
	public SchemaManipulator unwrapArrays(int ignoredMaxSuffixLength) {
		unwrapTests.add((path, schema, wrapping, wrapped) -> {
			String wrappingName = wrapping.name();
			String wrappedName = wrapped.name();
			// Determine the length of the shortest name to calculate the length of the prefix that must be equal.
			int prefixLength = Math.max(0, Math.min(wrappingName.length(), wrappedName.length()) - ignoredMaxSuffixLength);
			return wrappingName.substring(0, prefixLength).equals(wrappedName.substring(0, prefixLength));
		});
		return this;
	}

	/**
	 * <p>Unwrap the array in the specified wrapping field.</p>
	 *
	 * <p>Wrapped arrays are an XML construct. They result in array fields without siblings in a record field (optionally in a union with null). In Avro,
	 * Parquet, and in fact most/all other formats, they are both not needed and unwanted. This method unwraps them based on the name of the wrapping field and
	 * the name of the schema that contains it.</p>
	 *
	 * <p>When unwrapping, wrapped field will replace the wrapping field using the name of the wrapping field. As this is not a renaming action, no alias will
	 * be added.</p>
	 *
	 * @param schemaName    the name of the schema with the wrapping field
	 * @param wrappingField the wrapping field; it'll get the
	 * @return this {@code SchemaManipulator}
	 * @see #unwrapArrays(int)
	 * @see #unwrapArrayAtPath(String...)
	 */
	public SchemaManipulator unwrapArray(String schemaName, String wrappingField) {
		unwrapTests.add((path, schema, wrapping, wrapped) ->
				isOneOf(schemaName, schema.getFullName(), schema.getAliases()) &&
				isOneOf(wrappingField, wrapping.name(), wrapping.aliases())
		);
		return this;
	}

	/**
	 * <p>Unwrap the array whose wrapping field is at the specified path.</p>
	 *
	 * <p>Wrapped arrays are an XML construct. They result in array fields without siblings in a record field (optionally in a union with null). In Avro,
	 * Parquet, and in fact most/all other formats, they are both not needed and unwanted. This method unwraps them based on the path to the wrapping field.</p>
	 *
	 * <p>When unwrapping, wrapped field will replace the wrapping field using the name of the wrapping field. As this is not a renaming action, no alias will
	 * be added.</p>
	 *
	 * @param pathToWrappingField the path to the wrapping field
	 * @return this {@code SchemaManipulator}
	 * @see #unwrapArrays(int)
	 * @see #unwrapArray(String, String)
	 */
	public SchemaManipulator unwrapArrayAtPath(String... pathToWrappingField) {
		String pathToMatch = String.join(".", pathToWrappingField);
		unwrapTests.add((p, s, wr, wd) -> p.equals(pathToMatch));
		return this;
	}

	/**
	 * <p>Document the schema as Markdown table when completing the schema manipulation.</p>
	 *
	 * <p>The table is written into the provided {@code StringBuilder}.</p>
	 *
	 * @return this {@code SchemaManipulator}
	 */
	public SchemaManipulator alsoDocumentAsMarkdownTable(StringBuilder buffer) {
		markdownBuffer = buffer;
		return this;
	}

	/**
	 * <p>Finish the schema manipulation, but discard the resulting schema and return the Markdown table documenting the schema instead.</p>
	 *
	 * <p>After calling this method, the schema manipulator remains usable. You can get the resulting schema by calling {@link #finish()}.</p>
	 *
	 * @return the Markdown table documenting the schema.
	 * @see #finish()
	 */
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
