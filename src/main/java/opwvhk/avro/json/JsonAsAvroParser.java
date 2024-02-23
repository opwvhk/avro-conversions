package opwvhk.avro.json;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jimblackler.jsonschemafriend.ValidationException;
import net.jimblackler.jsonschemafriend.Validator;
import opwvhk.avro.ResolvingFailure;
import opwvhk.avro.io.AsAvroParserBase;
import opwvhk.avro.io.ListResolver;
import opwvhk.avro.io.RecordResolver;
import opwvhk.avro.io.ScalarValueResolver;
import opwvhk.avro.io.ValueResolver;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static opwvhk.avro.util.AvroSchemaUtils.nonNullableSchemaOf;

/**
 * <p>JSON parser to read Avro records.</p>
 *
 * <p>This parser requires at least an Avro schema, and yields a parser that can parse any JSON that has a compatible structure.</p>
 *
 * <p>When reading JSON, any conversion that "fits" is supported. Apart from exact (scalar) type matches and matching record fields by name/alias, this
 * includes widening conversions and enum values as string. Additionally, any number can be read as float/double (losing some precision).</p>
 *
 * <p>Note that without a JSON schema, all downsides listed in {@link AsAvroParserBase#createResolver(Schema)} apply.</p>
 */
public class JsonAsAvroParser extends AsAvroParserBase<SchemaProperties> {

	private final ObjectMapper mapper;
	private final Validator validator;

	private static Predicate<SchemaProperties> jsonType(SchemaType... types) {
		return w -> {
			EnumSet<SchemaType> allowedTypes = w.types();
			for (SchemaType type : types) {
				if (allowedTypes.contains(type)) {
					return true;
				}
			}
			return false;
		};
	}

	private static Predicate<SchemaProperties> isNumber() {
		return jsonType(SchemaType.NUMBER, SchemaType.INTEGER);
	}

	private static Predicate<SchemaProperties> isInteger(int bitSize) {
		return w -> w.types().contains(SchemaType.INTEGER) &&
		            w.numberRange().integerBitSize() <= bitSize;
	}

	private static Predicate<SchemaProperties> hasStringFormat(String format) {
		return w -> format.equals(w.format());
	}

	private static Predicate<SchemaProperties> hasEncodedContent(String contentEncoding) {
		return w -> contentEncoding.equals(w.contentEncoding());
	}

	private static boolean isValidDecimal(SchemaProperties writeType, Schema readSchema) {
		return isNumber().test(writeType) &&
		       readSchema.getLogicalType() instanceof LogicalTypes.Decimal readDecimal &&
		       readDecimal.getPrecision() >= writeType.numberRange().requiredPrecision() &&
		       readDecimal.getScale() >= writeType.numberRange().requiredScale();
	}

	private static boolean isValidEnum(SchemaProperties writeType, Schema readSchema) {
		Set<String> enumValues = writeType.enumValues();
		// Not an issue: enums are generally not that large
		// noinspection SlowListContainsAll
		return enumValues != null &&
		       readSchema.getType() == Schema.Type.ENUM &&
		       (readSchema.getEnumDefault() != null ||
		        readSchema.getEnumSymbols().containsAll(enumValues));
	}

	private final ValueResolver resolver;
	private final net.jimblackler.jsonschemafriend.Schema jsonSchema;

	/**
	 * Create a strict JSON parser, validating input against a JSON schema, and reading it using the specified Avro schema.
	 *
	 * @param jsonSchemaLocation the location of the JSON (write) schema (schema of the JSON data to parse)
	 * @param readSchema         the read schema (schema of the resulting records)
	 * @param model              the Avro model used to create records
	 */
	public JsonAsAvroParser(URI jsonSchemaLocation, Schema readSchema, GenericData model) {
		this(jsonSchemaLocation, true, readSchema, model);
	}

	/**
	 * Create a JSON parser using only the specified Avro schema. The parse result will match the schema, but can be invalid if {@code validateInput} is set to
	 * {@code false}.
	 *
	 * @param jsonSchemaLocation the location of the JSON (write) schema (schema of the JSON data to parse)
	 * @param validateInput      if {@code true}, validate the input when parsing
	 * @param readSchema         the read schema (schema of the resulting records)
	 * @param model              the Avro model used to create records
	 */
	public JsonAsAvroParser(URI jsonSchemaLocation, boolean validateInput, Schema readSchema, GenericData model) {
		this(model, analyseJsonSchema(jsonSchemaLocation), readSchema, validateInput);
	}

	private static SchemaProperties analyseJsonSchema(URI jsonSchemaLocation) {
		SchemaAnalyzer schemaAnalyzer = new SchemaAnalyzer();
        return schemaAnalyzer.parseJsonProperties(jsonSchemaLocation);
	}

	/**
	 * Create a JSON parser using only the specified Avro schema. The parse result will match the schema, but might be invalid: no check is done that all
	 * required fields have a value.
	 *
	 * @param readSchema the read schema (schema of the resulting records)
	 * @param model      the Avro model used to create records
	 */
	public JsonAsAvroParser(Schema readSchema, GenericData model) {
		this(model, null, readSchema, false);
	}

	private JsonAsAvroParser(GenericData model, SchemaProperties schemaProperties, Schema readSchema, boolean validateInput) {
		super(model);
		resolver = createResolver(schemaProperties, readSchema);
		mapper = new ObjectMapper();
		if (validateInput) {
			jsonSchema = schemaProperties.getJsonSchema();
			validator = new Validator(false);
		} else {
			jsonSchema = null;
			validator = null;
		}
	}

	@Override
	protected List<ResolveRule<SchemaProperties>> createResolveRules() {
		List<ResolveRule<SchemaProperties>> resolveRules = super.createResolveRules();

		// Logical types
		resolveRules.add(new ResolveRule<>(JsonAsAvroParser::isValidDecimal, (w, r) -> createDecimalResolver(r)));
		resolveRules.add(new ResolveRule<>(hasStringFormat("date"), logicalType(LogicalTypes.Date.class), (w, r) -> LOCAL_DATE_RESOLVER));
		resolveRules.add(new ResolveRule<>(hasStringFormat("time"), logicalType(LogicalTypes.TimeMillis.class), (w, r) -> offsetTimeResolver));
		resolveRules.add(new ResolveRule<>(hasStringFormat("time"), logicalType(LogicalTypes.TimeMicros.class), (w, r) -> offsetTimeResolver));
		resolveRules.add(new ResolveRule<>(hasStringFormat("date-time"), logicalType(LogicalTypes.TimestampMillis.class), (w, r) -> instantResolver));
		resolveRules.add(new ResolveRule<>(hasStringFormat("date-time"), logicalType(LogicalTypes.TimestampMicros.class), (w, r) -> instantResolver));
		resolveRules.add(
				new ResolveRule<>(hasStringFormat("date-time"), logicalType(LogicalTypes.LocalTimestampMillis.class), (w, r) -> LOCAL_DATE_TIME_RESOLVER));
		resolveRules.add(
				new ResolveRule<>(hasStringFormat("date-time"), logicalType(LogicalTypes.LocalTimestampMicros.class), (w, r) -> LOCAL_DATE_TIME_RESOLVER));
		// Raw scalar types (note: as logical types, binary types and enums are _also_ strings, strings must be last)
		resolveRules.add(new ResolveRule<>(hasEncodedContent("base16"), rawType(Schema.Type.BYTES),
				(w, r) -> new ScalarValueResolver(text -> ByteBuffer.wrap(new BigInteger(text, 16).toByteArray()))));
		resolveRules.add(new ResolveRule<>(hasEncodedContent("base64"), rawType(Schema.Type.BYTES),
				(w, r) -> new ScalarValueResolver(text -> ByteBuffer.wrap(Base64.getDecoder().decode(text)))));
		resolveRules.add(new ResolveRule<>(JsonAsAvroParser::isValidEnum, (w, r) -> createEnumResolver(r)));
		resolveRules.add(new ResolveRule<>(jsonType(SchemaType.BOOLEAN), rawType(Schema.Type.BOOLEAN), (w, r) -> BOOLEAN_RESOLVER));
		resolveRules.add(new ResolveRule<>(isNumber(), rawType(Schema.Type.FLOAT), (w, r) -> FLOAT_RESOLVER));
		resolveRules.add(new ResolveRule<>(isNumber(), rawType(Schema.Type.DOUBLE), (w, r) -> DOUBLE_RESOLVER));
		resolveRules.add(new ResolveRule<>(isInteger(32), rawType(Schema.Type.INT), (w, r) -> INTEGER_RESOLVER));
		resolveRules.add(new ResolveRule<>(isInteger(64), rawType(Schema.Type.LONG), (w, r) -> LONG_RESOLVER));
		resolveRules.add(new ResolveRule<>(jsonType(SchemaType.STRING), rawType(Schema.Type.STRING), (w, r) -> STRING_RESOLVER));
		// Composite types
		// UNION is not needed: this is unwrapped as needed (and forced if the JSON may contain explicit null values)
		resolveRules.add(new ResolveRule<>(jsonType(SchemaType.ARRAY), rawType(Schema.Type.ARRAY),
				(w, r) -> new ListResolver(createResolver(w.itemSchemaProperties(), nonNullableSchemaOf(r.getElementType())))));
		// resolveRules.add(new ResolveRule<>(JsonAsAvroParser::isValidEnum, (w, r) -> createRecordResolver(r)));
		resolveRules.add(new ResolveRule<>(jsonType(SchemaType.OBJECT), rawType(Schema.Type.RECORD), this::createResolverForRecord));

		return resolveRules;
	}

	protected ValueResolver createResolver(SchemaProperties writerProperties, Schema readSchema) {
		// If there is a write schema, we can check if data may be missing.
		if (writerProperties != null &&
		    writerProperties.isNullable() &&
		    !readSchema.isNullable()) {
			throw new ResolvingFailure(
					"The write schema allows null, but the read schema doesn't. Cannot map %s to %s".formatted(writerProperties, readSchema));
		}

		return super.createResolver(writerProperties, nonNullableSchemaOf(readSchema));
	}

	private ValueResolver createResolverForRecord(SchemaProperties writerProperties, Schema readSchema) {
		Map<String, Schema.Field> readFieldsByName = collectFieldsByNameAndAliases(readSchema);
		Set<Schema.Field> unhandledButRequiredFields = determineRequiredFields(readSchema);

		RecordResolver recordResolver = new RecordResolver(model, readSchema);

		for (Map.Entry<String, SchemaProperties> entry : writerProperties.properties().entrySet()) {
			String name = entry.getKey();
			boolean requiredField = writerProperties.requiredProperties().contains(name);
			Schema.Field readField = readFieldsByName.get(name);
			if (readField == null) {
				continue; // No such field: skip
			}
			unhandledButRequiredFields.remove(readField);

			SchemaProperties fieldProperties = entry.getValue();
			if (!requiredField && !readField.hasDefaultValue()) {
				throw new ResolvingFailure("JSON field '%s' is not required, but the corresponding Avro field '%s' has no default in schema %s"
						.formatted(name, readField.name(), readSchema));
			}
			ValueResolver fieldResolver = createResolver(fieldProperties, readField.schema());
			recordResolver.addResolver(name, readField, fieldResolver);
		}

		return recordResolver;
	}

	/**
	 * Parse the given source into records.
	 *
	 * @param <T>    the record type
	 * @param source JSON data that was read already
	 * @return the parsed record
	 * @throws IOException when the JSON cannot be read
	 */
	public <T> T parse(String source) throws IOException {
		try (JsonParser parser = mapper.createParser(source)) {
			return parse(parser);
		}
	}

	/**
	 * Parse the given source into records.
	 *
	 * @param url a location to read JSON data from
	 * @param <T> the record type
	 * @return the parsed record
	 * @throws IOException when the JSON cannot be read
	 */
	public <T> T parse(URL url) throws IOException {
		try (JsonParser parser = mapper.createParser(url)) {
			return parse(parser);
		}
	}

	private <T> T parse(JsonParser parser) throws IOException {
		try {
			ResolvingJsonParser<T> resolvingJsonParser = new ResolvingJsonParser<>(parser, resolver);
			if (jsonSchema == null) {
				while (resolvingJsonParser.nextToken() != null) {
					// Do nothing: we've wrapped the parser to do the work.
				}
			} else {
				Object object = resolvingJsonParser.readValueAs(Object.class);
				validator.validate(jsonSchema, object);
			}
			return resolvingJsonParser.result;
		} catch (ValidationException e) {
			throw new IOException("Invalid JSON", e);
		}
	}

	/**
	 * <p>Create a parser that creates the intended parse result while we're parsing the input for validation.</p>
	 *
	 * <p>The reason for this is that all available JSON validation libraries require some form of parsed in-memory representation to validate.
	 * JSON schemas offer too many matching options for conditionals, even against property types and values, to make streaming validation feasible.</p>
	 *
	 * <p>This also means that parsing without a schema is more efficient (uses less memory and causes less GC pressure).</p>
	 */
	private static class ResolvingJsonParser<T> extends JsonParserDelegate {
		private final CollectingResolver noopResolver;
		private final Deque<CollectingResolver> parseStack;
		private T result;

		private ResolvingJsonParser(JsonParser parser, ValueResolver rootResolver) {
			super(parser);
			noopResolver = new CollectingResolver(ValueResolver.NOOP);
			parseStack = new ArrayDeque<>();
			parseStack.push(new CollectingResolver(rootResolver));
			result = null;
		}

		@Override
		public JsonToken nextToken() throws IOException {
			JsonToken jsonToken = super.nextToken();
			if (jsonToken != null) {
				// We'll never get these values:
				// 'NOT_AVAILABLE' -> only returned by non-blocking parsers, which we don't use
				// 'VALUE_EMBEDDED_OBJECT' -> no known parser ever returns this
				CollectingResolver collectingResolver = parseStack.element();
				switch (jsonToken) {
					case START_OBJECT:
						if (collectingResolver.isCollectingRecord()) {
							parseStack.push(noopResolver);
						} else {
							JsonLocation location = getTokenLocation();
							throw new IllegalStateException("Did not expect an object at %d:%d".formatted(location.getLineNr(), location.getColumnNr()));
						}
						break;
					case START_ARRAY:
						if (collectingResolver.isCollectingArray()) {
							parseStack.push(collectingResolver.resolve("value")); // Any value will do
						} else {
							JsonLocation location = getTokenLocation();
							throw new IllegalStateException("Did not expect an array at %d:%d".formatted(location.getLineNr(), location.getColumnNr()));
						}
						break;
					case FIELD_NAME:
						parseStack.pop();
						// collectingResolver is the one we just popped; we need its parent
						parseStack.push(parseStack.element().resolve(currentName()));
						break;
					default:
						collectingResolver.addContent(getValueAsString());
						parseStack.push(noopResolver);
						// Fall through
					case END_OBJECT:
					case END_ARRAY:
						// First pop the dummy/item/field resolver that is used for child elements, but we no longer need that.
						parseStack.pop();
						// Then pop the collector for our value, and peek ahead for the collector of our parent.
						CollectingResolver valueResolver = parseStack.pop();
						// Here, we want both the current top collector (the parent), and collector we just popped (that collected our value).
						// Note we've first popped a dummy/item/field resolver that is used for child elements, if present, but we no longer need that.
						Object value = valueResolver.complete();
						CollectingResolver parentResolver = parseStack.peek();
						if (parentResolver == null) {
							result = (T) value;
						} else if (parentResolver.addProperty(currentName(), value)) {
							parseStack.push(parentResolver.resolve("value")); // Any value will do
						} else {
							parseStack.push(noopResolver);
						}
						break;
				}
			}
			return jsonToken;
		}
	}

	private static class CollectingResolver {
		private final ValueResolver resolver;
		private Object collector;

		private CollectingResolver(ValueResolver resolver) {
			this.resolver = requireNonNull(resolver);
			this.collector = resolver.createCollector();
		}

		private CollectingResolver resolve(String fieldName) {
			return new CollectingResolver(resolver.resolve(fieldName));
		}

		private void addContent(String value) {
			collector = resolver.addContent(collector, value);
		}

		private Object complete() {
			return resolver.complete(collector);
		}

		private boolean addProperty(String fieldName, Object value) {
			resolver.addProperty(collector, fieldName, value);
			return isCollectingArray();
		}

		private boolean isCollectingArray() {
			return resolver instanceof ListResolver;
		}

		private boolean isCollectingRecord() {
			return resolver instanceof RecordResolver;
		}
	}
}
