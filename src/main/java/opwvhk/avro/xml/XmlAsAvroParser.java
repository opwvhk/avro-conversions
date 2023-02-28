package opwvhk.avro.xml;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import opwvhk.avro.datamodel.Cardinality;
import opwvhk.avro.datamodel.DecimalType;
import opwvhk.avro.datamodel.EnumType;
import opwvhk.avro.datamodel.FixedType;
import opwvhk.avro.datamodel.ScalarType;
import opwvhk.avro.datamodel.StructType;
import opwvhk.avro.datamodel.Type;
import opwvhk.avro.datamodel.TypeWithUnparsedContent;
import opwvhk.avro.io.ListResolver;
import opwvhk.avro.io.RecordResolver;
import opwvhk.avro.io.ResolvingFailure;
import opwvhk.avro.io.ScalarValueResolver;
import opwvhk.avro.io.ValueResolver;
import opwvhk.avro.xsd.XsdAnalyzer;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static java.util.Objects.requireNonNull;

public class XmlAsAvroParser {
	private static final EnumSet<FixedType> BINARY_TYPES = EnumSet.of(FixedType.BINARY_HEX, FixedType.BINARY_BASE64);
	private static final EnumSet<FixedType> FLOATING_POINT_TYPES = EnumSet.of(FixedType.FLOAT, FixedType.DOUBLE);
	@SuppressWarnings("SlowListContainsAll") // Not an issue: enums are generally not that large
	private static final List<Rule> SCALAR_RESOLVE_RULES = List.of(
			new Rule((t1, t2) -> t1 instanceof FixedType && t1 == t2, (s, r, w, m) -> resolverForScalarType(s, r, m)),
			new Rule(t -> t == FixedType.STRING, EnumType.class::isInstance, (s, r, w, m) -> resolverForScalarType(s, r, m)),
			new Rule(cast(EnumType.class, (r, w) -> r.defaultSymbol() != null || r.enumSymbols().containsAll(w.enumSymbols())),
					(s, r, w, m) -> resolverForScalarType(s, r, m)),
			new Rule(cast(DecimalType.class, (r, w) -> r.scale() >= w.scale() && r.precision() >= w.precision()), (s, r, w, m) -> resolverForScalarType(s, r, m)),
			new Rule(FLOATING_POINT_TYPES::contains, t -> t == FixedType.FLOAT || t instanceof DecimalType, (s, r, w, m) -> resolverForScalarType(s, r, m)),
			// Note: Parse binary data using write type (this differentiates between hex & base64 encoding): they yield the same result (a ByteBuffer)
			new Rule(BINARY_TYPES::contains, BINARY_TYPES::contains, (s, r, w, m) -> resolverForScalarType(s, w, m))
	);

	private static ValueResolver resolverForScalarType(Schema readSchema, Type readingType, GenericData model) {
		Function<String, Object> converter = ((ScalarType)readingType)::parse;

		if (readingType instanceof EnumType) {
			converter = converter.andThen(symbol -> model.createEnum(symbol.toString(), readSchema));
		}

		LogicalType logicalType = readSchema.getLogicalType();
		Conversion<Object> conversion = model.getConversionFor(logicalType);
		if (conversion != null) {
			// If there is no conversion, treat the logical type as informative.
			converter = converter.andThen(parsed -> Conversions.convertToRawType(parsed, readSchema, logicalType, conversion));
		}

		return new ScalarValueResolver(converter);
	}

	static <T extends ScalarType> BiPredicate<ScalarType, Type> cast(Class<T> clazz, BiPredicate<T, T> predicate) {
		return (o1, o2) -> (clazz.isInstance(o1) && clazz.isInstance(o2)) && predicate.test(clazz.cast(o1), clazz.cast(o2));
	}

	private final SAXParser parser;

	private final ValueResolver resolver;

	public XmlAsAvroParser(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		this(xsdLocation.toExternalForm(), createResolver(xsdLocation, rootElement, readSchema, model));
	}

	private static ValueResolver createResolver(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(xsdLocation);
		Type writeType = xsdAnalyzer.typeOf(rootElement);

		ensureConversionFor(model, LogicalTypes.date(), TimeConversions.DateConversion::new);
		ensureConversionFor(model, LogicalTypes.timeMillis(), TimeConversions.TimeMillisConversion::new);
		ensureConversionFor(model, LogicalTypes.timeMicros(), TimeConversions.TimeMicrosConversion::new);
		ensureConversionFor(model, LogicalTypes.timestampMillis(), TimeConversions.TimestampMillisConversion::new);
		ensureConversionFor(model, LogicalTypes.timestampMicros(), TimeConversions.TimestampMicrosConversion::new);
		ensureConversionFor(model, LogicalTypes.decimal(1,1), Conversions.DecimalConversion::new);
		return resolve(readSchema, Type.fromSchema(readSchema), writeType, model);
	}

	private static void ensureConversionFor(GenericData model, LogicalType logicalType, Supplier<Conversion<?>> conversionSupplier) {
		if (model.getConversionFor(logicalType) == null) {
			model.addLogicalTypeConversion(conversionSupplier.get());
		}
	}

	static ValueResolver resolve(Schema readSchema, Type readType, Type writeType, GenericData model) {
		boolean hasUnparsedContent = false;
		if (writeType instanceof TypeWithUnparsedContent typeWithUnparsedContent) {
			writeType = typeWithUnparsedContent.actualType();
			hasUnparsedContent = true;
		}

		ValueResolver resolver;
		if (readType instanceof ScalarType scalarReadType) {
			resolver = resolve(readSchema, scalarReadType, writeType, model);
		} else if (writeType instanceof StructType structWriteType) {
			StructType structReadType = (StructType) readType; // Only other option
			resolver = resolve(readSchema, structReadType, structWriteType, model);
		} else {
			throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeType, readType));
		}
		if (hasUnparsedContent) {
			resolver.doNotParseContent();
		}
		return resolver;
	}

	private static ValueResolver resolve(Schema readSchema, StructType readType, StructType writeType, GenericData model) {
		List<StructType.Field> readFields = readType.fields();
		Set<StructType.Field> requiredFields = readFields.stream().filter(f -> f.defaultValue() == null).collect(Collectors.toSet());
		RecordResolver resolver = new RecordResolver(model, readSchema);
		for (StructType.Field readField : readFields) {
			StructType.Field writeField = writeType.getField(readField.name(), readField.aliases());
			if (writeField != null) {
				requiredFields.remove(readField);
				Schema.Field readSchemaField = readSchema.getField(readField.name());
				Cardinality readCardinality = readField.cardinality();
				Schema readSchemaFieldSchema = switch (readCardinality) {
					case MULTIPLE -> readSchemaField.schema().getElementType();
					case OPTIONAL -> readSchemaField.schema().getTypes().stream().filter(s -> !s.isNullable()).findAny().orElseThrow();
					default -> readSchemaField.schema();
				};
				ValueResolver fieldResolver = resolve(readSchemaFieldSchema, readCardinality, readField.type(), writeField.cardinality(),
						writeField.type(), model);
				if (readCardinality == Cardinality.MULTIPLE && !(fieldResolver instanceof ListResolver)) {
					resolver.addArrayResolver(writeField.name(), readSchemaField.pos(), fieldResolver);
				} else {
					resolver.addResolver(writeField.name(), readSchemaField.pos(), fieldResolver);
				}
			}
		}
		if (requiredFields.isEmpty()) {
			return resolver;
		}
		throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeType, readType));
	}

	private static ValueResolver resolve(Schema readSchema, Cardinality readCardinality, Type readType, Cardinality writeCardinality, Type writeType,
	                                     GenericData model) {
		// Special case: handle wrapped arrays in XML. The recursive call enforces that the wrapped field must be an array.
		if (readCardinality == Cardinality.MULTIPLE &&
		    writeCardinality != Cardinality.MULTIPLE &&
		    !isStructTypeWithSingleField(readType) &&
		    isStructTypeWithSingleField(writeType)) {
			StructType.Field writeField = ((StructType) writeType).fields().get(0);
			ValueResolver nestedResolver = resolve(readSchema, readCardinality, readType, writeField.cardinality(), writeField.type(), model);
			return new ListResolver(nestedResolver);
		}
		if (readCardinality.compareTo(writeCardinality) >= 0) {
			return resolve(readSchema, readType, writeType, model);
		}
		throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeCardinality.formatName(writeType.toString()),
				readCardinality.formatName(readType.toString())));
	}

	private static boolean isStructTypeWithSingleField(Type type) {
		return type instanceof StructType structType && structType.fields().size() == 1;
	}

	private static ValueResolver resolve(Schema readSchema, ScalarType readType, Type writeType, GenericData model) {
		for (Rule rule : SCALAR_RESOLVE_RULES) {
			if (rule.test(readType, writeType)) {
				return requireNonNull(rule.createResolver(readSchema, readType, writeType, model));
			}
		}
		throw new ResolvingFailure("Cannot convert data written as %s into %s".formatted(writeType, readType));
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

	public <T> T parse(InputSource source) throws IOException, SAXException {
		XmlRecordHandler handler = new XmlRecordHandler(resolver);
		parser.parse(source, new SimpleContentAdapter(handler));
		return handler.getValue();
	}

	public <T> T parse(URL url) throws IOException, SAXException {
		InputSource inputSource = new InputSource();
		inputSource.setSystemId(url.toExternalForm());
		return parse(inputSource);
	}

	private record Rule(BiPredicate<ScalarType, Type> testReadAndWriteTypes, ResolverFactory resolverFactory)
			implements ResolverFactory, BiPredicate<ScalarType, Type> {
		private Rule(Predicate<ScalarType> testReadType, Predicate<Type> testWriteType, ResolverFactory resolverFactory) {
			this((r, w) -> testReadType.test(r) && testWriteType.test(w), resolverFactory);
		}

		@Override
		public boolean test(ScalarType readType, Type writeType) {
			return testReadAndWriteTypes().test(readType, writeType);
		}

		@Override
		public ValueResolver createResolver(Schema readSchema, ScalarType readType, Type writeType, GenericData model) {
			return resolverFactory().createResolver(readSchema, readType, writeType, model);
		}
	}

	private interface ResolverFactory {
		ValueResolver createResolver(Schema readSchema, ScalarType readType, Type writeType, GenericData model);
	}
}
