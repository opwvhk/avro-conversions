package opwvhk.avro.xml;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalQuery;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import opwvhk.avro.ResolvingFailure;
import opwvhk.avro.xml.datamodel.Cardinality;
import opwvhk.avro.xml.datamodel.DecimalType;
import opwvhk.avro.xml.datamodel.EnumType;
import opwvhk.avro.xml.datamodel.FixedType;
import opwvhk.avro.xml.datamodel.StructType;
import opwvhk.avro.xml.datamodel.Type;
import opwvhk.avro.xml.datamodel.TypeWithUnparsedContent;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * <p>XML parser to read Avro records.</p>
 *
 * <p>This parser requires an XSD and Avro schema, and yields a parser that correctly parses the XML, including binary data, unwrapping
 * arrays, etc. into an Avro record that adheres to the specified schema.</p>
 *
 * <p>When reading XML, any conversion that "fits" is supported. Apart from exact (scalar) type matches and matching record fields by name/alias, this includes
 * widening conversions, reading single values as array and enum values as string. Additionally, any number can be read as float/double (losing some
 * precision).
 * </p>
 */
public class XmlAsAvroParser {
	private static final EnumSet<FixedType> FLOATING_POINT_TYPES = EnumSet.of(FixedType.FLOAT, FixedType.DOUBLE);
	/**
	 * Date format as specified for XML.
	 */
	private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4)
			.appendLiteral("-")
			.appendValue(ChronoField.MONTH_OF_YEAR, 2)
			.appendLiteral("-")
			.appendValue(ChronoField.DAY_OF_MONTH, 2)
			.toFormatter(Locale.ROOT);
	/**
	 * Time format as specified for XML, but limited to nanoseconds (XML specified no limit to the precision).
	 */
	private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
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
			.toFormatter(Locale.ROOT)
			.withZone(UTC);
	/**
	 * DateTime format as specified for XML, but limited to nanoseconds (XML specified no limit to the precision).
	 */
	private static final DateTimeFormatter DATE_TIME_FORMAT = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.append(DATE_FORMAT)
			.optionalStart()
			.appendLiteral("T")
			.optionalEnd()
			.optionalStart()
			.appendLiteral(" ")
			.optionalEnd()
			.append(TIME_FORMAT)
			.toFormatter(Locale.ROOT)
			.withZone(UTC);
	private static final List<Rule> SCALAR_RESOLVE_RULES = List.of(
			// Simple scalar types
			new Rule(rawType(Schema.Type.BOOLEAN), t -> t == FixedType.BOOLEAN, (r, w, m) -> new ScalarValueResolver(Boolean::valueOf)),
			new Rule(rawType(Schema.Type.FLOAT), t -> t == FixedType.FLOAT, (r, w, m) -> new ScalarValueResolver(Float::valueOf)),
			new Rule(rawType(Schema.Type.FLOAT), DecimalType.class::isInstance, (r, w, m) -> new ScalarValueResolver(Float::valueOf)),
			new Rule(rawType(Schema.Type.DOUBLE), FLOATING_POINT_TYPES::contains, (r, w, m) -> new ScalarValueResolver(Double::valueOf)),
			new Rule(rawType(Schema.Type.DOUBLE), DecimalType.class::isInstance, (r, w, m) -> new ScalarValueResolver(Double::valueOf)),
			new Rule(rawType(Schema.Type.STRING), t -> t == FixedType.STRING, (r, w, m) -> new ScalarValueResolver(s -> s)),
			// Enums (also as string)
			new Rule(XmlAsAvroParser::isValidEnum, (r, w, m) -> new ScalarValueResolver(sym -> enumSymbol(sym, r, m))),
			new Rule(rawType(Schema.Type.STRING), EnumType.class::isInstance, (r, w, m) -> new ScalarValueResolver(s -> s)),
			// Fixed-point number types
			new Rule(rawType(Schema.Type.INT), decimal(32), (r, w, m) -> new ScalarValueResolver(Integer::decode)),
			new Rule(rawType(Schema.Type.LONG), decimal(64), (r, w, m) -> new ScalarValueResolver(Long::decode)),
			new Rule(XmlAsAvroParser::isValidDecimal, (r, w, m) -> new ScalarValueResolver(decimalParser(r, m))),
			// Date & time types: the read schema decides the precision (milliseconds or microseconds)
			new Rule(logicalType(LogicalTypes.Date.class), t -> t == FixedType.DATE,
					(r, w, m) -> new ScalarValueResolver(str -> convert(m, r, LocalDate.parse(str, DATE_FORMAT)))),
			new Rule(logicalType(LogicalTypes.TimeMillis.class), t -> t == FixedType.TIME,
					(r, w, m) -> new ScalarValueResolver(str -> convert(m, r, OffsetTime.parse(str, TIME_FORMAT).truncatedTo(ChronoUnit.MILLIS)))),
			new Rule(logicalType(LogicalTypes.TimeMicros.class), t -> t == FixedType.TIME,
					(r, w, m) -> new ScalarValueResolver(str -> convert(m, r, OffsetTime.parse(str, TIME_FORMAT).truncatedTo(ChronoUnit.MICROS)))),
			new Rule(logicalType(LogicalTypes.TimestampMillis.class), t -> t == FixedType.DATETIME,
					(r, w, m) -> new ScalarValueResolver(str -> convert(m, r, ZonedDateTime.parse(str, DATE_TIME_FORMAT).toInstant()
							.truncatedTo(ChronoUnit.MILLIS)))),
			new Rule(logicalType(LogicalTypes.TimestampMicros.class), t -> t == FixedType.DATETIME,
					(r, w, m) -> new ScalarValueResolver(str -> convert(m, r, ZonedDateTime.parse(str, DATE_TIME_FORMAT).toInstant()
									.truncatedTo(ChronoUnit.MICROS)))),
			// Binary types: the XML decides how to parse them (hex or base64)
			new Rule(rawType(Schema.Type.BYTES), t -> t == FixedType.BINARY_HEX, (r, w, m) -> new ScalarValueResolver(FixedType.BINARY_HEX::parse)),
			new Rule(rawType(Schema.Type.BYTES), t -> t == FixedType.BINARY_BASE64, (r, w, m) -> new ScalarValueResolver(FixedType.BINARY_BASE64::parse))
	);

	private static Predicate<Schema> rawType(Schema.Type type) {
		return s -> s.getLogicalType() == null && s.getType() == type;
	}

	private static Predicate<Schema> logicalType(Class<? extends LogicalType> logicalTypeClass) {
		return s -> s.getLogicalType() != null && logicalTypeClass.isInstance(s.getLogicalType());
	}

	private static boolean isValidEnum(Schema readSchema, Type writeType) {
		// Not an issue: enums are generally not that large
		//noinspection SlowListContainsAll
		return writeType instanceof EnumType writeEnum && readSchema.getType() == Schema.Type.ENUM &&
		       (readSchema.getEnumDefault() != null || readSchema.getEnumSymbols().containsAll(writeEnum.enumSymbols()));
	}

	private static Object enumSymbol(String input, Schema enumSchema, GenericData model) {
		// As the XML was validated before parsing, we know this code will work
		String symbol;
		if (enumSchema.getEnumSymbols().contains(input)) {
			symbol = input;
		} else {
			symbol = requireNonNull(enumSchema.getEnumDefault(), "Invalid symbol for enum without default: " + input);
		}
		return model.createEnum(symbol, enumSchema);
	}

	private static Predicate<Type> decimal(int maxBitSize) {
		return t -> t instanceof DecimalType dt &&
		            dt.bitSize() <= maxBitSize;
	}

	private static boolean isValidDecimal(Schema readSchema, Type writeType) {
		return readSchema.getLogicalType() instanceof LogicalTypes.Decimal readDecimal
		       &&
		       writeType instanceof DecimalType writeDecimal &&
		       readDecimal.getPrecision() >= writeDecimal.precision() &&
		       readDecimal.getScale() >= writeDecimal.scale();
	}

	private static Function<String, Object> decimalParser(Schema readSchema, GenericData model) {
		LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) readSchema.getLogicalType();
		int scale = logicalType.getScale();
		// Note: as the XML was validated before parsing, we're certain the precision is not too large.
		return text -> convert(model, readSchema, new BigDecimal(text).setScale(scale, RoundingMode.UNNECESSARY));
	}

	private static Object convert(GenericData model, Schema schemaWithLogicalType, Object value) {
		LogicalType logicalType = schemaWithLogicalType.getLogicalType();
		Conversion<?> conversion = model.getConversionByClass(value.getClass(), logicalType);
		return Conversions.convertToRawType(value, schemaWithLogicalType, logicalType, conversion);
	}

	private static <T, U> T parseJavaTime(String text, DateTimeFormatter format, TemporalQuery<T> preferred, TemporalQuery<U> fallback,
	                                      Function<U, T> conversion) {
		try {
			return format.parse(text, preferred);
		} catch (RuntimeException firstException) {
			try {
				return conversion.apply(format.parse(text, fallback));
			} catch (RuntimeException e) {
				firstException.addSuppressed(e);
				throw firstException;
			}
		}
	}

	private final SAXParser parser;

	private final ValueResolver resolver;

	/**
	 * <p>Create an XML parser for the specified XSD and root element, reading data into records created by the model for the given read schema.</p>
	 *
	 * <p>The resulting parser can read any data, also invalid data, as long as it fits the result. Be aware though, that parsing invalid data is likely to
	 * result in invalid records. These may/will cause unspecified problems downstream.</p>
	 *
	 * @param xsdLocation the XSD defining the data to read
	 * @param rootElement the root element that will be read
	 * @param readSchema  the schema of the resulting records
	 * @param model       the model to create records
	 * @throws IOException when the XSD cannot be read
	 */
	public XmlAsAvroParser(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		this(xsdLocation.toExternalForm(), createResolver(xsdLocation, rootElement, readSchema, model));
	}

	private static ValueResolver createResolver(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(xsdLocation);
		Type writeType = xsdAnalyzer.typeOf(rootElement);

		ensureConversionFor(model, LogicalTypes.date(), LocalDate.class, TimeConversions.DateConversion::new);
		ensureConversionFor(model, LogicalTypes.timestampMillis(), Instant.class, TimeConversions.TimestampMillisConversion::new);
		ensureConversionFor(model, LogicalTypes.timestampMicros(), Instant.class, TimeConversions.TimestampMicrosConversion::new);
		ensureConversionFor(model, LogicalTypes.decimal(1, 1), BigDecimal.class, Conversions.DecimalConversion::new);
		// Note: for each of the logical types, the last conversion becomes the default for the logical type (if queried without class).
		ensureConversionFor(model, LogicalTypes.timeMillis(), LocalTime.class, TimeConversions.TimeMillisConversion::new);
		ensureConversionFor(model, LogicalTypes.timeMillis(), OffsetTime.class, AvroConversions.OffsetTimeMillisConversion::new);
		ensureConversionFor(model, LogicalTypes.timeMicros(), LocalTime.class, TimeConversions.TimeMicrosConversion::new);
		ensureConversionFor(model, LogicalTypes.timeMicros(), OffsetTime.class, AvroConversions.OffsetTimeMicrosConversion::new);
		return resolve(writeType, readSchema, model);
	}

	private static void ensureConversionFor(GenericData model, LogicalType logicalType, Class<?> valueClass, Supplier<Conversion<?>> conversionSupplier) {
		if (model.getConversionByClass(valueClass, logicalType) == null) {
			model.addLogicalTypeConversion(conversionSupplier.get());
		}
	}

	static ValueResolver resolve(Type writeType, Schema readSchema, GenericData model) {
		boolean hasUnparsedContent = writeType instanceof TypeWithUnparsedContent;
		// with unparsed content, writeType is either a FixedType.STRING, or a StructType with a field named "value" that has that type
		Type structOrScalarWriteType = hasUnparsedContent ? ((TypeWithUnparsedContent) writeType).actualType() : writeType;

		Schema nonNullableReadSchema = nonNullableSchemaOf(readSchema);

		ValueResolver resolver;
		if (!(structOrScalarWriteType instanceof StructType structWriteType)) {
			resolver = resolveScalar(nonNullableReadSchema, structOrScalarWriteType, model);
		} else if (nonNullableReadSchema.getType() == Schema.Type.RECORD) {
			resolver = resolveRecord(structWriteType, nonNullableReadSchema, model);
		} else {
			throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(structOrScalarWriteType, nonNullableReadSchema));
		}
		if (hasUnparsedContent) {
			resolver.doNotParseContent();
		}
		return resolver;
	}

	private static Schema nonNullableSchemaOf(Schema readSchema) {
		if (readSchema.isUnion()) {
			return readSchema.getTypes().stream().filter(s -> !s.isNullable()).findAny().orElseThrow();
		} else {
			return readSchema;
		}
	}

	private static ValueResolver resolveRecord(StructType writeType, Schema readSchema, GenericData model) {
		List<Schema.Field> readFields = readSchema.getFields();
		Map<String, Schema.Field> readFieldsByName = new HashMap<>();
		Set<Schema.Field> unhandledButRequiredFields = new HashSet<>();
		for (Schema.Field readField : readFields) {
			if (!readField.hasDefaultValue()) {
				unhandledButRequiredFields.add((readField));
			}
			readFieldsByName.put(readField.name(), readField);
			readField.aliases().forEach(alias -> readFieldsByName.put(alias, readField));
		}

		RecordResolver resolver = new RecordResolver(model, readSchema);
		for (StructType.Field writeField : writeType.fields()) {
			Schema.Field readField = readFieldsByName.get(writeField.name());
			if (readField == null) {
				continue; // No such field: skip
			}

			unhandledButRequiredFields.remove(readField);

			ValueResolver fieldResolver = resolveField(writeField, readField, model);
			if (readField.schema().getType() == Schema.Type.ARRAY && !(fieldResolver instanceof ListResolver)) {
				resolver.addArrayResolver(writeField.name(), readField.pos(), fieldResolver);
			} else {
				resolver.addResolver(writeField.name(), readField.pos(), fieldResolver);
			}
		}
		if (unhandledButRequiredFields.isEmpty()) {
			return resolver;
		}
		throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeType, readSchema));
	}

	private static ValueResolver resolveField(StructType.Field writeField, Schema.Field readField, GenericData model) {
		boolean readFieldIsArray = readField.schema().getType() == Schema.Type.ARRAY;
		Cardinality writeCardinality = writeField.cardinality();
		if (writeCardinality == Cardinality.MULTIPLE) {
			if (!readFieldIsArray) {
				throw new ResolvingFailure("Field must be an array: cannot convert data written as %s into %s".formatted(writeField, readField));
			}
			return resolve(writeField.type(), getFieldElementSchema(readField), model);
		} else {
			if (readFieldIsArray) {
				Schema elementSchema = getFieldElementSchema(readField);
				if (writeField.type() instanceof StructType writeStructType && writeStructType.fields().size() == 1 &&
				    (elementSchema.getType() != Schema.Type.RECORD || elementSchema.getFields().size() != 1)) {
					// Special case: handle wrapped arrays in XML. The recursive call enforces that the wrapped field must be an array.
					writeField = writeStructType.fields().get(0);
					ValueResolver nestedResolver = resolve(writeField.type(), elementSchema, model);
					return new ListResolver(nestedResolver);
				}
				return resolve(writeField.type(), elementSchema, model);
			}
			if (writeCardinality == Cardinality.OPTIONAL && !readField.hasDefaultValue()) {
				throw new ResolvingFailure(
						"Field may be absent, but there is no default: cannot convert data written as %s into %s".formatted(writeField, readField));
			}
			Schema readFieldSchema = nonNullableSchemaOf(readField.schema());
			return resolve(writeField.type(), readFieldSchema, model);
		}
	}

	private static Schema getFieldElementSchema(Schema.Field readField) {
		Schema elementSchema = nonNullableSchemaOf(readField.schema().getElementType());
		if (elementSchema.getType() == Schema.Type.ARRAY) {
			throw new ResolvingFailure("Nested arrays are not supported.");
		}
		return elementSchema;
	}

	private static ValueResolver resolveScalar(Schema readSchema, Type writeType, GenericData model) {
		for (Rule rule : SCALAR_RESOLVE_RULES) {
			if (rule.test(readSchema, writeType)) {
				return requireNonNull(rule.createResolver(readSchema, writeType, model));
			}
		}
		throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeType, readSchema));
	}

	XmlAsAvroParser(String xsdLocation, ValueResolver resolver) {
		parser = createParser(xsdLocation);
		this.resolver = resolver;
	}

	private SAXParser createParser(String xsdLocation) {
		try {
			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			parserFactory.setNamespaceAware(true);

			SchemaFactory schemaFactory = SchemaFactory.newDefaultInstance();
			javax.xml.validation.Schema schema = schemaFactory.newSchema(new StreamSource(xsdLocation));
			parserFactory.setSchema(schema);

			return parserFactory.newSAXParser();
		} catch (SAXException | ParserConfigurationException e) {
			throw new IllegalStateException("Failed to create parser", e);
		}
	}

	/**
	 * Parse the given source into records.
	 *
	 * @param source     a source of XML data
	 * @param enforceXsd if {@code true}, parsing will fail if the XML is not valid (this includes a missing namespace)
	 * @param <T>        the record type
	 * @return the parsed record
	 * @throws IOException  when the XML cannot be read
	 * @throws SAXException when the XML cannot be parsed
	 */
	public <T> T parse(InputSource source, boolean enforceXsd) throws IOException, SAXException {
		XmlRecordHandler handler = new XmlRecordHandler(resolver);
		parser.parse(source, new SimpleContentAdapter(handler, enforceXsd));
		return handler.getValue();
	}

	/**
	 * Parse the given source into records. Does not enforce the XSD.
	 *
	 * @param url a location to read XML data from
	 * @param <T> the record type
	 * @return the parsed record
	 * @throws IOException  when the XML cannot be read
	 * @throws SAXException when the XML cannot be parsed
	 */
	public <T> T parse(URL url) throws IOException, SAXException {
		InputSource inputSource = new InputSource();
		inputSource.setSystemId(url.toExternalForm());
		return parse(inputSource, false);
	}

	private record Rule(BiPredicate<Schema, Type> testReadAndWriteTypes, ResolverFactory resolverFactory)
			implements ResolverFactory, BiPredicate<Schema, Type> {
		private Rule(Predicate<Schema> testReadType, Predicate<Type> testWriteType, ResolverFactory resolverFactory) {
			this((r, w) -> testReadType.test(r) && testWriteType.test(w), resolverFactory);
		}

		@Override
		public boolean test(Schema readType, Type writeType) {
			return testReadAndWriteTypes().test(readType, writeType);
		}

		@Override
		public ValueResolver createResolver(Schema readSchema, Type writeType, GenericData model) {
			return resolverFactory().createResolver(readSchema, writeType, model);
		}
	}

	private interface ResolverFactory {
		ValueResolver createResolver(Schema readSchema, Type writeType, GenericData model);
	}
}
