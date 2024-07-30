package opwvhk.avro.io;

import opwvhk.avro.ResolvingFailure;
import opwvhk.avro.util.AvroConversions;
import opwvhk.avro.util.Utils;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Base16;
import org.jetbrains.annotations.VisibleForTesting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static opwvhk.avro.util.AvroSchemaUtils.nonNullableSchemaOf;

/**
 * <p>Base class to create parsers for data formats into Avro.</p>
 *
 * <p>Subclasses should implement a </p>
 */
public abstract class AsAvroParserBase<WriteSchema> {
	/**
	 * Date format as specified by ISO8601.
	 */
	private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4)
			.appendLiteral("-")
			.appendValue(ChronoField.MONTH_OF_YEAR, 2)
			.appendLiteral("-")
			.appendValue(ChronoField.DAY_OF_MONTH, 2)
			.toFormatter(Locale.ROOT);
	private static final DateTimeFormatter ZONE_LESS_TIME_FORMATTER = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.appendValue(ChronoField.HOUR_OF_DAY, 2)
			.appendLiteral(":")
			.appendValue(ChronoField.MINUTE_OF_HOUR, 2)
			.appendLiteral(":")
			.appendValue(ChronoField.SECOND_OF_MINUTE, 2)
			.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
			.optionalStart()
			.appendZoneOrOffsetId()
			.optionalEnd()
			.toFormatter(Locale.ROOT);
	private static final DateTimeFormatter ZONE_LESS_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.append(DATE_FORMAT)
			.optionalStart()
			.appendLiteral("T")
			.optionalEnd()
			.optionalStart()
			.appendLiteral(" ")
			.optionalEnd()
			.append(ZONE_LESS_TIME_FORMATTER)
			.toFormatter(Locale.ROOT);
	/**
	 * Resolver for boolean values.
	 */
	protected static final ScalarValueResolver BOOLEAN_RESOLVER = new ScalarValueResolver(Boolean::valueOf);
	/**
	 * Resolver for single precision floating point values.
	 */
	protected static final ScalarValueResolver FLOAT_RESOLVER = new ScalarValueResolver(Float::valueOf);
	/**
	 * Resolver for double precision floating point values.
	 */
	protected static final ScalarValueResolver DOUBLE_RESOLVER = new ScalarValueResolver(Double::valueOf);
	/**
	 * Resolver for 32-bit signed integer values.
	 */
	protected static final ScalarValueResolver INTEGER_RESOLVER = new ScalarValueResolver(Integer::decode);
	/**
	 * Resolver for 64-bit signed integer (long) values.
	 */
	protected static final ScalarValueResolver LONG_RESOLVER = new ScalarValueResolver(Long::decode);
	/**
	 * Resolver for string values.
	 */
	protected static final ScalarValueResolver STRING_RESOLVER = new ScalarValueResolver(s -> s);
	/**
	 * Resolver for base 16 formatted binary values.
	 */
	protected static final ScalarValueResolver BASE16_RESOLVER = new ScalarValueResolver(text -> ByteBuffer.wrap(new Base16().decode(text)));
	/**
	 * Resolver for base 64 formatted binary values.
	 */
	protected static final ScalarValueResolver BASE64_RESOLVER = new ScalarValueResolver(text -> ByteBuffer.wrap(Base64.getDecoder().decode(text)));
	/**
	 * Resolver for ISO8601 (local) dates (using {@link DateTimeFormatter#ISO_DATE}).
	 */
	protected static final ScalarValueResolver LOCAL_DATE_RESOLVER = new ScalarValueResolver(text -> LocalDate.parse(text, DATE_FORMAT));
	/**
	 * Resolver for local date-times (using {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME}).
	 */
	protected static final ScalarValueResolver LOCAL_DATE_TIME_RESOLVER = new ScalarValueResolver(LocalDateTime::parse);

	/**
	 * <p>Determine the offset for a time zone.</p>
	 *
	 * <p>If not an offset already, uses the specified clock and zone rules to determine the offset.</p>
	 *
	 * <p>This method is visible for testing, as the default implementations only ever use UTC.</p>
	 *
	 * @param timezone a timezone
	 * @param clock    the (system) clock
	 * @return the zone offset to use
	 */
	static ZoneOffset asOffset(ZoneId timezone, Clock clock) {
		if (timezone instanceof ZoneOffset offset) {
			return offset;
		} else {
			return timezone.getRules().getOffset(clock.instant());
		}
	}

	/**
	 * Returns a test if a schema has a specific raw type. Utility predicate to use when implementing {@link #createResolveRules()}.
	 *
	 * @param type the schema type we're interested in
	 * @return predicate that tests if an Avro schema as the specified raw type
	 */
	protected static Predicate<Schema> rawType(Schema.Type type) {
		return s -> s.getLogicalType() == null && s.getType() == type;
	}

	/**
	 * Returns a test if a schema has a specific logical type. Utility predicate to use when implementing {@link #createResolveRules()}.
	 *
	 * @param logicalTypeClass the logical type we're interested in
	 * @return predicate that tests if an Avro schema as the specified logical type
	 */
	protected static Predicate<Schema> logicalType(Class<? extends LogicalType> logicalTypeClass) {
		return s -> logicalTypeClass.isInstance(s.getLogicalType());
	}

	protected final GenericData model;
	private final Set<Schema.Field> fieldsAllowedMissing;
	/**
	 * Resolver for ISO8601 times (format {@code HH:mm:ss[,SSS][V]}). When a timezone is not specified, the default timezone will be used. Note that times are
	 * parsed up to nanosecond level, even though many parsed formats allow any precision.
	 */
	protected final ScalarValueResolver offsetTimeResolver;
	/**
	 * Resolver for ISO8601 timestamps. When a timezone is not specified, the default timezone will be used. Note that times are parsed up to nanosecond level,
	 * even though many parsed formats allow any precision.
	 */
	protected final ScalarValueResolver instantResolver;
	private final List<ResolveRule<WriteSchema>> resolveRules;
	private final ValueResolver resolver;

	/**
	 * Create an {@code AsAvroParserBase}, using the specified model, write and read schemata, and the {@code UTC} time zone as default.
	 *
	 * @param model       the model to create records and enum symbols with
	 * @param writeSchema the write schema to parse
	 * @param readSchema  the read schema to parse into
	 * @see #AsAvroParserBase(GenericData, WriteSchema, Schema, Set<Schema.Field>, ZoneId)
	 */
	@SuppressWarnings({"JavadocDeclaration"}) // Bug: marks generics as illegal, but current javadoc tool requires it
	protected AsAvroParserBase(GenericData model, WriteSchema writeSchema, Schema readSchema) {
		this(model, writeSchema, readSchema, Set.of(), UTC);
	}

	/**
	 * <p>Create an {@code AsAvroParserBase}, using the specified model, write and read schemata, and the {@code UTC} time zone as default.</p>
	 *
	 * @param model                the model to create records and enum symbols with
	 * @param writeSchema          the write schema to parse
	 * @param readSchema           the read schema to parse into
	 * @param fieldsAllowedMissing fields in the read schema that are allowed to be missing, even when this yields invalid records
	 * @see #AsAvroParserBase(GenericData, WriteSchema, Schema, Set<Schema.Field>, ZoneId)
	 */
	@SuppressWarnings({"UnnecessaryJavaDocLink", "JavadocDeclaration"}) // Bug: marks generics as illegal, but current javadoc tool requires it
	protected AsAvroParserBase(GenericData model, WriteSchema writeSchema, Schema readSchema, Set<Schema.Field> fieldsAllowedMissing) {
		this(model, writeSchema, readSchema, fieldsAllowedMissing, UTC);
	}

	/**
	 * <p>Create an {@code AsAvroParserBase}, using the specified model, schemas and default time zone.</p>
	 *
	 * <p>
	 * You can specify fields from the read schema that are allowed to have missing values regardless of what the read schema says. If the write schema does
	 * not guarantee a value that the read schema requires, this normally leads to an exception. You can whitelist fields for which this should not happen.
	 * <strong>Note:</strong> this can lead to invalid records, so you must take care to fill in the missing values before the records are serialized!
	 * </p>
	 *
	 * <p>
	 * The fallback time zone will be used when parsing times and timestamps that do not contain a timezone. Because times do not carry date information,
	 * daylight saving cannot be determined. To solve this, they will be parsed as if they're today (the day this parser is created).
	 * </p>
	 *
	 * @param model                the model to create records and enum symbols with
	 * @param writeSchema          the write schema to parse
	 * @param readSchema           the read schema to parse into
	 * @param fieldsAllowedMissing fields in the read schema that are allowed to be missing, even when this yields invalid records
	 * @param fallbackTimezone     the fallback time zone to use when parsing times and timestamps
	 */
	protected AsAvroParserBase(GenericData model, WriteSchema writeSchema, Schema readSchema, Set<Schema.Field> fieldsAllowedMissing,
	                           ZoneId fallbackTimezone) {
		this.model = model;
		this.fieldsAllowedMissing = fieldsAllowedMissing;
		DateTimeFormatter timeFormat = ZONE_LESS_TIME_FORMATTER.withZone(asOffset(fallbackTimezone, Clock.systemDefaultZone()));
		offsetTimeResolver = new ScalarValueResolver(text -> OffsetTime.parse(text, timeFormat));
		DateTimeFormatter dateTimeFormat = ZONE_LESS_DATE_TIME_FORMATTER.withZone(fallbackTimezone);
		instantResolver = new ScalarValueResolver(text -> ZonedDateTime.parse(text, dateTimeFormat).toInstant());

		ensureConversionFor(LogicalTypes.decimal(1, 1), BigDecimal.class, Conversions.DecimalConversion::new);
		ensureConversionFor(LogicalTypes.date(), LocalDate.class, TimeConversions.DateConversion::new);
		ensureConversionFor(LogicalTypes.timestampMillis(), Instant.class, TimeConversions.TimestampMillisConversion::new);
		ensureConversionFor(LogicalTypes.timestampMicros(), Instant.class, TimeConversions.TimestampMicrosConversion::new);
		ensureConversionFor(LogicalTypes.localTimestampMillis(), LocalDateTime.class, TimeConversions.LocalTimestampMillisConversion::new);
		ensureConversionFor(LogicalTypes.localTimestampMicros(), LocalDateTime.class, TimeConversions.LocalTimestampMicrosConversion::new);
		// Note: for each of the logical types, the last conversion becomes the default for the logical type (if queried without class).
		ensureConversionFor(LogicalTypes.timeMillis(), LocalTime.class, TimeConversions.TimeMillisConversion::new);
		ensureConversionFor(LogicalTypes.timeMillis(), OffsetTime.class, AvroConversions.OffsetTimeMillisConversion::new);
		ensureConversionFor(LogicalTypes.timeMicros(), LocalTime.class, TimeConversions.TimeMicrosConversion::new);
		ensureConversionFor(LogicalTypes.timeMicros(), OffsetTime.class, AvroConversions.OffsetTimeMicrosConversion::new);

		resolveRules = createResolveRules();
		resolver = createResolver(writeSchema, readSchema);
	}

	/**
	 * Collect all fields in a record schema, mapped by their names (i.e., name and aliases).
	 *
	 * @param recordSchema a record schema
	 * @return all fields, mapped by all their names
	 */
	protected static Map<String, Schema.Field> collectFieldsByNameAndAliases(Schema recordSchema) {
		Map<String, Schema.Field> readFieldsByName = new HashMap<>();
		for (Schema.Field readField : recordSchema.getFields()) {
			readFieldsByName.put(readField.name(), readField);
			readField.aliases().forEach(alias -> readFieldsByName.put(alias, readField));
		}
		return readFieldsByName;
	}

	/**
	 * Determine all required fields of a record schema.
	 *
	 * @param recordSchema a record schema
	 * @return a mutable set with all required fields
	 */
	protected Set<Schema.Field> determineRequiredFields(Schema recordSchema) {
		Set<Schema.Field> unhandledButRequiredFields = new HashSet<>();
		for (Schema.Field readField : recordSchema.getFields()) {
			if (!fieldsAllowedMissing.contains(readField) && !readField.hasDefaultValue()) {
				unhandledButRequiredFields.add(readField);
			}
		}
		return unhandledButRequiredFields;
	}

	private void ensureConversionFor(LogicalType logicalType, Class<?> valueClass, Supplier<Conversion<?>> conversionSupplier) {
		if (model.getConversionByClass(valueClass, logicalType) == null) {
			model.addLogicalTypeConversion(conversionSupplier.get());
		}
	}

	/**
	 * <p>Create the resolver rules to use when creating a resolver for a pair of write &amp; read schemata.</p>
	 *
	 * <p>These rules are used in the method {@link #createResolver(Object, Schema) createResolver(WriteSchema, Schema)}, and are explicitly encouraged to
	 * call this method to resolve elements of composite types (it guards against infinite recursion).</p>
	 *
	 * <p>When overriding this method, please call it first: the default implementation returns a mutable list of rules that resolve a read schema against a
	 * {@code null} (absent) write schema. Ignoring the default implementation removes that functionality.</p>
	 *
	 * <p>The rules returned by the default implementation match all possible read schemata. If you add your rules after them, you're guaranteed that they'll
	 * never receive a {@code null} write schema, even if your implementation allows creating resolvers for a read schema only.
	 *
	 * @return the resolve rules for this parser
	 * @see #createResolver(Object, Schema) createResolver(WriteSchema, Schema)
	 */
	protected List<ResolveRule<WriteSchema>> createResolveRules() {
		List<ResolveRule<WriteSchema>> resolveRules = new ArrayList<>();

		EnumSet<Schema.Type> unsupportedTypesForNullWriteType = EnumSet.of(Schema.Type.MAP, Schema.Type.FIXED, Schema.Type.BYTES, Schema.Type.NULL);
		ResolverFactory<WriteSchema> throwForUnsupportedType = (w, r) -> {
			throw new ResolvingFailure("Without a write type, %s data is not supported".formatted(r.getType().getName().toLowerCase(Locale.ROOT)));
		};

		// Logical types
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.Decimal.class), (w, r) -> createDecimalResolver(r)));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.Date.class), (w, r) -> LOCAL_DATE_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.TimeMillis.class), (w, r) -> offsetTimeResolver));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.TimeMicros.class), (w, r) -> offsetTimeResolver));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.TimestampMillis.class), (w, r) -> instantResolver));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.TimestampMicros.class), (w, r) -> instantResolver));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.LocalTimestampMillis.class), (w, r) -> LOCAL_DATE_TIME_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, logicalType(LogicalTypes.LocalTimestampMicros.class), (w, r) -> LOCAL_DATE_TIME_RESOLVER));
		// Raw scalar types
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.BOOLEAN), (w, r) -> BOOLEAN_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.FLOAT), (w, r) -> FLOAT_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.DOUBLE), (w, r) -> DOUBLE_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.INT), (w, r) -> INTEGER_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.LONG), (w, r) -> LONG_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.STRING), (w, r) -> STRING_RESOLVER));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.ENUM), (w, r) -> createEnumResolver(r)));
		Predicate<Schema> binaryTypeWithFormat = rawType(Schema.Type.BYTES).or(rawType(Schema.Type.FIXED)).and(s -> s.getProp("format") != null);
		resolveRules.add(new ResolveRule<>(Objects::isNull, binaryTypeWithFormat, (w, r) -> createBinaryResolver(r)));
		// Composite types
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.UNION), (w, r) -> createResolver(null, nonNullableSchemaOf(r))));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.ARRAY),
				(w, r) -> new ListResolver(createResolver(null, nonNullableSchemaOf(r.getElementType())))));
		resolveRules.add(new ResolveRule<>(Objects::isNull, rawType(Schema.Type.RECORD), (w, r) -> createRecordResolver(r)));
		// Explicitly unsupported types (needed here to weed out null write types, and to add a better error message)
		resolveRules.add(new ResolveRule<>(Objects::isNull, r -> unsupportedTypesForNullWriteType.contains(r.getType()), throwForUnsupportedType));

		return resolveRules;
	}

	/**
	 * Get the value resolver to use when parsing.
	 *
	 * @return the value resolver
	 */
	protected ValueResolver getResolver() {
		return resolver;
	}

	private static final ThreadLocal<Map<Utils.Seen, ValueResolver>> SEEN = ThreadLocal.withInitial(HashMap::new);

	/**
	 * <p>Create a {@code ValueResolver} that can resolve written values in the write schema into parsed values in the read schema.</p>
	 *
	 * <p>This method uses the rules returned by {@link #createResolveRules()}. Please note that these rules are explicitly encouraged to use this method to
	 * resolve elements of composite types. This method guards against infinite recursion, by using a delegating {@code ValueResolver} that receives a delegate
	 * before returning.</p>
	 *
	 * @param writeSchema the schema of the written data
	 * @param readSchema  the schema to read the data as
	 * @return a resolver that can read written data into the read schema
	 * @see #createResolveRules()
	 */
	protected ValueResolver createResolver(WriteSchema writeSchema, Schema readSchema) {
		Map<Utils.Seen, ValueResolver> resolversForSeenSchemas = SEEN.get();
		boolean first = resolversForSeenSchemas.isEmpty();
		try {
			Utils.Seen schemaPair = new Utils.Seen(writeSchema, readSchema);
			ValueResolver previous = resolversForSeenSchemas.putIfAbsent(schemaPair, new DelegatingResolver());
			if (previous != null) {
				return previous;
			}

			for (ResolveRule<WriteSchema> rule : resolveRules) {
				if (rule.test(writeSchema, readSchema)) {
					// This method will (but may not) cause recursive calls to the current method.
					ValueResolver resolver = requireNonNull(rule.createResolver(writeSchema, readSchema));

					// The map contains the DelegatingResolver we put in above (otherwise the method would have exited directly after that).
					// Now we replace the DelegatingResolver with the created resolver, but we set it in the DelegatingResolver in case that was
					// used due to recursion.
					DelegatingResolver delegatingResolver = requireNonNull((DelegatingResolver) resolversForSeenSchemas.put(schemaPair, resolver));
					delegatingResolver.setDelegate(resolver);
					return resolver;
				}
			}
			throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeSchema, readSchema));
		} finally {
			if (first) {
				SEEN.remove();
			}
		}
	}

	/**
	 * <p>Create a resolver for a read schema, assuming the data that will be parsed is compatible.</p>
	 *
	 * <p>Using this method is an easy way to create a resolver, but has several downsides:</p>
	 *
	 * <ul>
	 *
	 * <li>
	 *     There is no early detection of incompatible types; exceptions due to incompatibilities will happen (at best) while parsing, but can occur later.
	 * </li>
	 *
	 * <li>
	 *     Specifically, it is impossible to determine if required fields may be missing. This will not cause problems when parsing, but will cause problems
	 *     (exceptions) when using the parse result at a later point in time.
	 * </li>
	 *
	 * <li>The resolver cannot handle binary data</li>
	 *
	 * </ul>
	 *
	 * @param readSchema the read schema (schema of the resulting records)
	 * @return a resolver that can parse compatible data
	 */
	protected ValueResolver createResolver(Schema readSchema) {
		return createResolver(null, readSchema);
	}

	/**
	 * Create a resolver for enumerated values.
	 *
	 * @param enumSchema an Avro schema with type {@link org.apache.avro.Schema.Type#ENUM}.
	 * @return a resolver for enumerated values
	 */
	protected ScalarValueResolver createEnumResolver(Schema enumSchema) {
		return new ScalarValueResolver(symbol -> enumSymbol(enumSchema, symbol));
	}

	private Object enumSymbol(Schema enumSchema, String input) {
		String symbol;
		if (enumSchema.getEnumSymbols().contains(input)) {
			symbol = input;
		} else {
			symbol = requireNonNull(enumSchema.getEnumDefault(), "Invalid symbol for enum without default: " + input);
		}
		return model.createEnum(symbol, enumSchema);
	}

	/**
	 * Create a resolver for decimal values.
	 *
	 * @param readSchema an Avro schema with logical type "decimal"
	 * @return a resolver for decimal values
	 */
	protected ScalarValueResolver createDecimalResolver(Schema readSchema) {
		return new ScalarValueResolver(decimalParser(readSchema));
	}

	private Function<String, Object> decimalParser(Schema readSchema) {
		LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) readSchema.getLogicalType();
		int scale = logicalType.getScale();
		// Note: as the XML was validated before parsing, we're certain the precision is not too large.
		return text -> new BigDecimal(text).setScale(scale, RoundingMode.UNNECESSARY);
	}


	/**
	 * Create a resolver for binary values, including a size check for fixed types.
	 *
	 * @param schema an Avro schema with type {@link org.apache.avro.Schema.Type#BYTES} or  {@link org.apache.avro.Schema.Type#FIXED}.
	 * @return a resolver for binary values
	 */
	@VisibleForTesting
	ScalarValueResolver createBinaryResolver(Schema schema) {
		Function<String, ByteBuffer> parser = switch (schema.getProp("format")) {
			case "base16" -> this::parseBase16;
			case "base64" -> this::parseBase64;
			default -> throw new ResolvingFailure("Unsupported format: " + schema.getProp("format"));
		};
		if (schema.getType() == Schema.Type.FIXED) {
			int size = schema.getFixedSize();
			parser = parser.andThen(bb -> {
				if (bb.capacity() != size) {
					throw new IllegalArgumentException("Wrong input size: expected %d bytes, but got %d bytes".formatted(size, bb.capacity()));
				}
				return bb;
			});
		}
		return new ScalarValueResolver(parser);
	}

	private ByteBuffer parseBase16(String text) {
		return ByteBuffer.wrap(new Base16().decode(text));
	}

	private ByteBuffer parseBase64(String text) {
		return ByteBuffer.wrap(Base64.getDecoder().decode(text));
	}

	private ValueResolver createRecordResolver(Schema readSchema) {
		RecordResolver resolver = new RecordResolver(model, readSchema);
		for (Schema.Field readField : readSchema.getFields()) {
			ValueResolver fieldResolver = createResolver(readField.schema());
			resolver.addResolver(readField.name(), readField, fieldResolver);
			readField.aliases().forEach(name -> resolver.addResolver(name, readField, fieldResolver));
		}
		return resolver;
	}

	/**
	 * Resolve rule for scalar values.
	 *
	 * @param testReadAndWriteTypes a predicate testing read and write types
	 * @param resolverFactory       a resolver factory to create a resolver if the predicate passes
	 * @param <WriteSchema>         the write schema type
	 */
	public record ResolveRule<WriteSchema>(BiPredicate<WriteSchema, Schema> testReadAndWriteTypes, ResolverFactory<WriteSchema> resolverFactory)
			implements ResolverFactory<WriteSchema>, BiPredicate<WriteSchema, Schema> {
		/**
		 * Resolve rule for scalar values.
		 *
		 * @param testWriteType   a predicate testing write types
		 * @param testReadType    a predicate testing read types
		 * @param resolverFactory a resolver factory to create a resolver if the predicates pass
		 */
		public ResolveRule(Predicate<WriteSchema> testWriteType, Predicate<Schema> testReadType, ResolverFactory<WriteSchema> resolverFactory) {
			this((r, w) -> testReadType.test(w) && testWriteType.test(r), resolverFactory);
		}

		public boolean test(WriteSchema writeType, Schema readType) {
			return testReadAndWriteTypes().test(writeType, readType);
		}

		@Override
		public ValueResolver createResolver(WriteSchema writeSchema, Schema readSchema) {
			return resolverFactory().createResolver(writeSchema, readSchema);
		}
	}

	/**
	 * Factory interface to create resolvers based on a read and write schema.
	 */
	public interface ResolverFactory<WriteSchema> {
		/**
		 * Create a resolver.
		 *
		 * @param writeSchema the write schema the data to resolve adheres to
		 * @param readSchema  the read schema to yield from the resolver
		 * @return the requested resolver
		 */
		ValueResolver createResolver(WriteSchema writeSchema, Schema readSchema);
	}
}
