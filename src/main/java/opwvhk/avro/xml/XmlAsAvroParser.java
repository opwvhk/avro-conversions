package opwvhk.avro.xml;

import opwvhk.avro.ResolvingFailure;
import opwvhk.avro.io.AsAvroParserBase;
import opwvhk.avro.io.ListResolver;
import opwvhk.avro.io.RecordResolver;
import opwvhk.avro.io.ScalarValueResolver;
import opwvhk.avro.io.ValueResolver;
import opwvhk.avro.util.AvroSchemaUtils;
import opwvhk.avro.xml.datamodel.Cardinality;
import opwvhk.avro.xml.datamodel.DecimalType;
import opwvhk.avro.xml.datamodel.EnumType;
import opwvhk.avro.xml.datamodel.FixedType;
import opwvhk.avro.xml.datamodel.StructType;
import opwvhk.avro.xml.datamodel.Type;
import opwvhk.avro.xml.datamodel.TypeWithUnparsedContent;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

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
public class XmlAsAvroParser extends AsAvroParserBase<Type> {
	private static final EnumSet<FixedType> FLOATING_POINT_TYPES = EnumSet.of(FixedType.FLOAT, FixedType.DOUBLE);

	private static boolean isValidEnum(Type writeType, Schema readSchema) {
		// Not an issue: enums are generally not that large
		// noinspection SlowListContainsAll
		return writeType instanceof EnumType writeEnum && readSchema.getType() == Schema.Type.ENUM &&
		       (readSchema.getEnumDefault() != null || readSchema.getEnumSymbols().containsAll(writeEnum.enumSymbols()));
	}

	private static Predicate<Type> decimal(int maxBitSize) {
		return t -> t instanceof DecimalType dt &&
		            dt.bitSize() <= maxBitSize;
	}

	private static boolean isValidDecimal(Type writeType, Schema readSchema) {
		return readSchema.getLogicalType() instanceof LogicalTypes.Decimal readDecimal &&
		       writeType instanceof DecimalType writeDecimal &&
		       readDecimal.getPrecision() >= writeDecimal.precision() &&
		       readDecimal.getScale() >= writeDecimal.scale();
	}

	private final SAXParser parser;

	private final ValueResolver resolver;

	/**
	 * <p>Create a validating XML parser for the specified XSD and root element, reading data into records created by the model for the given read schema.</p>
	 *
	 * <p>The parser is built to read XML that conforms to the XSD, yielding records in the read schema. It will fail to construct if the two are not
	 * compatible. Also, being a validating parser, it will fail to parse is the parsed XML does not conform to the XSD.</p>
	 *
	 * @param xsdLocation the XSD defining the data to read
	 * @param rootElement the root element that will be read
	 * @param readSchema  the schema of the resulting records
	 * @param model       the model to create records
	 * @throws IOException when the XSD cannot be read
	 */
	public XmlAsAvroParser(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		this(xsdLocation, rootElement, true, readSchema, model);
	}

	/**
	 * <p>Create an XML parser for the specified XSD and root element, reading data into records created by the model for the given read schema.</p>
	 *
	 * <p>The parser is built to read XML that conforms to the XSD, yielding records in the read schema. It will fail to construct if the two are not
	 * compatible.</p>
	 *
	 * <p>Note that validating the XML while parsing is optional. If {@code validate == false}, the resulting parser can read any XML data (also invalid data)
	 * as long as it fits the result. Be aware though, that parsing invalid data is likely to result in invalid records. These will cause unspecified problems
	 * when used.</p>
	 *
	 * @param xsdLocation the XSD defining the data to read
	 * @param rootElement the root element that will be read
	 * @param validate    whether the XML parser should validate XML while parsing
	 * @param readSchema  the schema of the resulting records
	 * @param model       the model to create records
	 * @throws IOException when the XSD cannot be read
	 */
	public XmlAsAvroParser(URL xsdLocation, String rootElement, boolean validate, Schema readSchema, GenericData model) throws IOException {
		this(model, xsdLocation, rootElement, validate, readSchema, null);
	}

	/**
	 * <p>Create an XML parser (just) the given read schema and model.</p>
	 *
	 * <p>The parser is built to read any XML, yielding records in the read schema. There is no validation (there's no XSD to validate against), and will read
	 * any XML data that fits into the read schema. Be aware though that there is also no way to detect missing required fields, so parsing invalid data is
	 * likely to result in invalid records. These will cause unspecified problems when used.</p>
	 *
	 * @param readSchema the schema of the resulting records
	 * @param model      the model to create records
	 */
	public XmlAsAvroParser(Schema readSchema, GenericData model) throws IOException {
		this(model, null, null, false, readSchema, null);
	}

	XmlAsAvroParser(GenericData model, URL xsdLocation, String rootElement, boolean validate, Schema readSchema, ValueResolver resolver) throws IOException {
		super(model);
		parser = createParser(validate ? xsdLocation : null);
		this.resolver = resolver != null ? resolver : createResolver(xsdLocation, rootElement, readSchema);
	}

	@Override
	protected List<ResolveRule<Type>> createResolveRules() {
		List<ResolveRule<Type>> resolveRules = super.createResolveRules();

		// Record types (arrays and unions are only supported in fields, and handled there)
		resolveRules.add(new ResolveRule<>(StructType.class::isInstance, rawType(Schema.Type.RECORD),
				(w, r) -> createResolverForRecord((StructType) w, r, model)));
		// Simple scalar types
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.BOOLEAN, rawType(Schema.Type.BOOLEAN), (w, r) -> BOOLEAN_RESOLVER));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.FLOAT, rawType(Schema.Type.FLOAT), (w, r) -> FLOAT_RESOLVER));
		resolveRules.add(new ResolveRule<>(DecimalType.class::isInstance, rawType(Schema.Type.FLOAT), (w, r) -> FLOAT_RESOLVER));
		resolveRules.add(new ResolveRule<>(FLOATING_POINT_TYPES::contains, rawType(Schema.Type.DOUBLE), (w, r) -> DOUBLE_RESOLVER));
		resolveRules.add(new ResolveRule<>(DecimalType.class::isInstance, rawType(Schema.Type.DOUBLE), (w, r) -> DOUBLE_RESOLVER));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.STRING, rawType(Schema.Type.STRING), (w, r) -> STRING_RESOLVER));
		// Enums (also as string)
		resolveRules.add(new ResolveRule<>(XmlAsAvroParser::isValidEnum, (w, r) -> createEnumResolver(r)));
		resolveRules.add(new ResolveRule<>(EnumType.class::isInstance, rawType(Schema.Type.STRING), (w, r) -> STRING_RESOLVER));
		// Fixed-point number types
		resolveRules.add(new ResolveRule<>(decimal(32), rawType(Schema.Type.INT), (w, r) -> INTEGER_RESOLVER));
		resolveRules.add(new ResolveRule<>(decimal(64), rawType(Schema.Type.LONG), (w, r) -> LONG_RESOLVER));
		resolveRules.add(new ResolveRule<>(XmlAsAvroParser::isValidDecimal, (w, r) -> createDecimalResolver(r)));
		// Date & time types: the read schema decides the precision (milliseconds or microseconds)
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.DATE, logicalType(LogicalTypes.Date.class), (w, r) -> LOCAL_DATE_RESOLVER));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.TIME, logicalType(LogicalTypes.TimeMillis.class), (w, r) -> offsetTimeResolver));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.TIME, logicalType(LogicalTypes.TimeMicros.class), (w, r) -> offsetTimeResolver));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.DATETIME, logicalType(LogicalTypes.TimestampMillis.class), (w, r) -> instantResolver));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.DATETIME, logicalType(LogicalTypes.TimestampMicros.class), (w, r) -> instantResolver));
		// Binary types: the XML decides how to parse them (hex or base64)
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.BINARY_HEX, rawType(Schema.Type.BYTES),
				(w, r) -> new ScalarValueResolver(FixedType.BINARY_HEX::parse)));
		resolveRules.add(new ResolveRule<>(t -> t == FixedType.BINARY_BASE64, rawType(Schema.Type.BYTES),
				(w, r) -> new ScalarValueResolver(FixedType.BINARY_BASE64::parse)));

		return resolveRules;
	}

	private ValueResolver createResolver(URL xsdLocation, String rootElement, Schema readSchema) throws IOException {
		if (xsdLocation == null) {
			return createResolver(readSchema);
		}
		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(xsdLocation);
		Type writeType = xsdAnalyzer.typeOf(rootElement);
		return createResolver(writeType, readSchema);
	}

	protected ValueResolver createResolver(Type writeType, Schema readSchema) {
		boolean hasUnparsedContent = writeType instanceof TypeWithUnparsedContent;
		// with unparsed content, writeType is either a FixedType.STRING, or a StructType with a field named "value" that has that type
		Type structOrScalarWriteType = hasUnparsedContent ? ((TypeWithUnparsedContent) writeType).actualType() : writeType;

		Schema nonNullableReadSchema = AvroSchemaUtils.nonNullableSchemaOf(readSchema);

		ValueResolver resolver = super.createResolver(structOrScalarWriteType, nonNullableReadSchema);

		if (hasUnparsedContent) {
			resolver.doNotParseContent();
		}
		return resolver;
	}

	private ValueResolver createResolverForRecord(StructType writeType, Schema readSchema, GenericData model) {
		Map<String, Schema.Field> readFieldsByName = collectFieldsByNameAndAliases(readSchema);
		Set<Schema.Field> unhandledButRequiredFields = determineRequiredFields(readSchema);

		RecordResolver resolver = new RecordResolver(model, readSchema);
		for (StructType.Field writeField : writeType.fields()) {
			Schema.Field readField = readFieldsByName.get(writeField.name());
			if (readField == null) {
				continue; // No such field: skip
			}

			unhandledButRequiredFields.remove(readField);

			ValueResolver fieldResolver = createResolverForField(writeField, readField);
			if (readField.schema().getType() == Schema.Type.ARRAY && !(fieldResolver instanceof ListResolver)) {
				resolver.addArrayResolver(writeField.name(), readField, fieldResolver);
			} else {
				resolver.addResolver(writeField.name(), readField, fieldResolver);
			}
		}
		if (unhandledButRequiredFields.isEmpty()) {
			return resolver;
		}
		throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeType, readSchema));
	}

	private ValueResolver createResolverForField(StructType.Field writeField, Schema.Field readField) {
		boolean readFieldIsArray = readField.schema().getType() == Schema.Type.ARRAY;
		Cardinality writeCardinality = writeField.cardinality();
		if (writeCardinality == Cardinality.MULTIPLE) {
			if (!readFieldIsArray) {
				throw new ResolvingFailure("Field must be an array: cannot convert data written as %s into %s".formatted(writeField, readField));
			}
			return createResolver(writeField.type(), getFieldElementSchema(readField));
		} else {
			if (readFieldIsArray) {
				Schema elementSchema = getFieldElementSchema(readField);
				if (writeField.type() instanceof StructType writeStructType && writeStructType.fields().size() == 1 &&
				    (elementSchema.getType() != Schema.Type.RECORD || elementSchema.getFields().size() != 1)) {
					// Special case: handle wrapped arrays in XML. The recursive call enforces that the wrapped field must be an array.
					writeField = writeStructType.fields().get(0);
					ValueResolver nestedResolver = createResolver(writeField.type(), elementSchema);
					return new ListResolver(nestedResolver);
				}
				return createResolver(writeField.type(), elementSchema);
			}
			if (writeCardinality == Cardinality.OPTIONAL && !readField.hasDefaultValue()) {
				throw new ResolvingFailure(
						"Field may be absent, but there is no default: cannot convert data written as %s into %s".formatted(writeField, readField));
			}
			Schema readFieldSchema = AvroSchemaUtils.nonNullableSchemaOf(readField.schema());
			return createResolver(writeField.type(), readFieldSchema);
		}
	}

	private Schema getFieldElementSchema(Schema.Field readField) {
		Schema elementSchema = AvroSchemaUtils.nonNullableSchemaOf(readField.schema().getElementType());
		if (elementSchema.getType() == Schema.Type.ARRAY) {
			throw new ResolvingFailure("Nested arrays are not supported.");
		}
		return elementSchema;
	}

	private SAXParser createParser(URL xsdLocation) {
		try {
			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			parserFactory.setNamespaceAware(true);

			if (xsdLocation != null) {
				SchemaFactory schemaFactory = SchemaFactory.newDefaultInstance();
				javax.xml.validation.Schema schema = schemaFactory.newSchema(new StreamSource(xsdLocation.toExternalForm()));
				parserFactory.setSchema(schema);
			}

			return parserFactory.newSAXParser();
		} catch (SAXException | ParserConfigurationException e) {
			throw new IllegalStateException("Failed to create parser", e);
		}
	}

	/**
	 * Parse the given source into records.
	 *
	 * @param source     a source of XML data
	 * @param <T>        the record type
	 * @return the parsed record
	 * @throws IOException  when the XML cannot be read
	 * @throws SAXException when the XML cannot be parsed
	 */
	public <T> T parse(InputSource source) throws IOException, SAXException {
		XmlRecordHandler handler = new XmlRecordHandler(resolver);
		parser.parse(source, new SimpleContentAdapter(handler));
		return handler.getValue();
	}

	/**
	 * Parse the given source into records.
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
		return parse(inputSource);
	}
}
