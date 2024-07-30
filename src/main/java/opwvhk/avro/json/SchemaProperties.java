package opwvhk.avro.json;

import net.jimblackler.jsonschemafriend.Schema;
import opwvhk.avro.util.DecimalRange;
import opwvhk.avro.util.Utils;

import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A class to hold properties of a JSON schema.
 */
public class SchemaProperties {
	private static final EnumSet<SchemaType> NON_NULL_TYPES = EnumSet.complementOf(EnumSet.of(SchemaType.NULL));
	private final boolean allowZeroFractionInIntegerRange;
	private Schema jsonSchema;
	private String inferredTitle;
	private String title;
	private String description;
	private EnumSet<SchemaType> types;
	private DecimalRange numberRange;
	private String format;
	private String contentEncoding;
	private Set<String> enumValues;
	private String defaultValue;
	private SchemaProperties itemSchemaProperties;
	private final Set<String> requiredProperties;
	private final Map<String, SchemaProperties> properties;

	/**
	 * Create empty JSON schema properties.
	 */
	public SchemaProperties(boolean allowZeroFractionInIntegerRange) {
		this.allowZeroFractionInIntegerRange = allowZeroFractionInIntegerRange;
		requiredProperties = new LinkedHashSet<>();
		properties = new LinkedHashMap<>();
	}

	/**
	 * Set the JSON schema these properties describe.
	 *
	 * @param jsonSchema a JSON schema
	 */
	public void setJsonSchema(Schema jsonSchema) {
		this.jsonSchema = jsonSchema;
	}

	/**
	 * Get the JSON schema these properties describe.
	 *
	 * @return the JSON schema
	 */
	public Schema getJsonSchema() {
		return jsonSchema;
	}

	/**
	 * Set the types supported by this schema.
	 *
	 * @param types the types to support
	 */
	public void setTypes(EnumSet<SchemaType> types) {
		this.types = types;
	}

	/**
	 * Add a type to the types supported by this schema.
	 *
	 * @param type the additional type to support
	 */
	public void addType(SchemaType type) {
		ensureTypes();
		types.add(type);
	}

	private void ensureTypes() {
		if (types == null) {
			types = EnumSet.noneOf(SchemaType.class);
		}
	}

	/**
	 * Determine if the JSON schema allows null values. Must not be called if no types have been set!
	 *
	 * @return whether the JSON schema allows null values
	 */
	public boolean isNullable() {
		ensureTypes();
		return types.contains(SchemaType.NULL);
	}

	/**
	 * Determine the most prominent supported schema type, if not {@link SchemaType#NULL}.
	 *
	 * @return the most prominent supported schema type except {@code NULL}
	 */
	public SchemaType getType() {
		ensureTypes();
		EnumSet<SchemaType> copy = types.clone();
		copy.retainAll(NON_NULL_TYPES);
		return copy.isEmpty() ? null : copy.iterator().next();
	}

	/**
	 * Types the JSON schema allows, if specified.
	 *
	 * @return the types the JSON schema allows, if specified
	 */
	public EnumSet<SchemaType> types() {
		return types;
	}

	/**
	 * The name of the JSON schema. Can be inferred from the structure (if there's no title property). Can be null, and is not guaranteed to be unique.
	 *
	 * @return the name of the JSON schema, if any
	 */
	public String title() {
		return title == null ? inferredTitle : title;
	}

	/**
	 * Set the name of the JSON schema.
	 *
	 * @param title the name of the JSON schema, if any
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * Set the inferred name of the JSON schema.
	 *
	 * @param inferredTitle the inferred name of the JSON schema, if any
	 */
	public void setInferredTitle(String inferredTitle) {
		this.inferredTitle = inferredTitle;
	}

	/**
	 * A description of the JSON schema, if available.
	 *
	 * @return a description of the JSON schema
	 */
	public String description() {
		return description;
	}

	/**
	 * Set the description of the JSON schema.
	 *
	 * @param description a description of the JSON schema, if any
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * If the JSON schema defines numbers, this range describes the allowed values.
	 *
	 * <p>If the schema type does not include numbers or integers, this value is undefined.</p>
	 *
	 * @return the range allowed for numbers
	 */
	public DecimalRange numberRange() {
		return numberRange;
	}

	/**
	 * If the JSON schema defines numbers, determine if the allowed number range is suitable for integers.
	 *
	 * <p>If the schema type does not include numbers or integers, this value is undefined.</p>
	 *
	 * @return whether the number range is suitable for integers
	 */
	public boolean isIntegerNumberRange() {
		return numberRange.isIntegerRange(allowZeroFractionInIntegerRange);
	}

	/**
	 * Set the range of allowed numbers.
	 *
	 * @param numberRange the range of allowed numbers
	 */
	public void setNumberRange(DecimalRange numberRange) {
		this.numberRange = numberRange;
	}

	/**
	 * For string types, describe the format.
	 *
	 * @return format for string types, if any
	 */
	public String format() {
		return format;
	}

	/**
	 * The name of the format string values follow.
	 *
	 * @param format a format name
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	/**
	 * For string values, describes how binary data is encoded.
	 *
	 * @return binary encoding to use interpret string values
	 */
	public String contentEncoding() {
		return contentEncoding;
	}

	/**
	 * Set the content encoding for binary values.
	 *
	 * @param contentEncoding the content encoding, like {@code base64}, {@code base16}, etc.
	 */
	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	/**
	 * Allowed values for enumerations, if any.
	 *
	 * @return all supported enum values
	 */
	public Set<String> enumValues() {
		return enumValues;
	}

	/**
	 * Set the enumeration values.
	 *
	 * @param enumValues the enum values
	 */
	public void setEnumValues(Set<String> enumValues) {
		this.enumValues = enumValues;
	}

	/**
	 * If not null, the default value for this JSON schema.
	 *
	 * @return the default value for this JSON schema, if available
	 */
	public String defaultValue() {
		return defaultValue;
	}

	/**
	 * Set the default value.
	 *
	 * @param defaultValue the default value
	 */
	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue == null ? null : defaultValue.toString();
	}

	/**
	 * For array schemas, the schema for its elements.
	 *
	 * @return schema for array items
	 */
	public SchemaProperties itemSchemaProperties() {
		return itemSchemaProperties;
	}

	/**
	 * Set the schema for array items.
	 *
	 * @param itemSchemaProperties the schema for array items
	 */
	public void setItemSchemaProperties(SchemaProperties itemSchemaProperties) {
		this.itemSchemaProperties = itemSchemaProperties;
	}

	/**
	 * For objects, the properties that are required.
	 *
	 * @return the required object properties
	 */
	public Set<String> requiredProperties() {
		return requiredProperties;
	}

	/**
	 * For objects, the properties that are defined.
	 *
	 * @return the properties for object schemas
	 */
	public Map<String, SchemaProperties> properties() {
		return properties;
	}

	/**
	 * Add an object property.
	 *
	 * @param name           the name of the property
	 * @param propertySchema the schema of the property
	 */
	public void addProperty(String name, SchemaProperties propertySchema) {
		properties.put(name, propertySchema);
	}

	@Override
	public String toString() {
		return Utils.nonRecursive("SchemaProperties.toString", this,
				"SchemaProperties@" + Integer.toHexString(System.identityHashCode(this)),
				() -> "SchemaProperties@" + Integer.toHexString(System.identityHashCode(this)) +
				      "{ title=" + q(title()) +
				      f(", description=", q(description)) +
				      f(", types=", types) +
				      f(", numberRange=", numberRange) +
				      f(", format=", q(format)) +
				      f(", contentEncoding=", q(contentEncoding)) +
				      f(", enumValues=", enumValues) +
				      f(", defaultValue=", q(defaultValue)) +
				      f(", itemSchemaProperties=", itemSchemaProperties) +
				      f(", requiredProperties=", requiredProperties) +
				      f(", properties=", properties) +
				      '}');
	}

	private String f(String prefix, Object value) {
		return value == null || (value instanceof Collection<?> c && c.isEmpty()) || (value instanceof Map<?, ?> m && m.isEmpty()) ? "" : prefix + value;
	}

	private String q(String s) {
		return s == null ? null : '\'' + s + '\'';
	}
}
