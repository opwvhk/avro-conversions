package opwvhk.avro;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import opwvhk.avro.io.ResolvingFailure;
import opwvhk.avro.xsd.Cardinality;
import opwvhk.avro.xsd.DecimalType;
import opwvhk.avro.xsd.EnumType;
import opwvhk.avro.xsd.FieldData;
import opwvhk.avro.xsd.FixedType;
import opwvhk.avro.xsd.ScalarType;
import opwvhk.avro.xsd.StructureBuilder;
import opwvhk.avro.xsd.TypeData;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.constants.Constants;
import org.apache.ws.commons.schema.utils.NamespacePrefixList;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.apache.ws.commons.schema.walker.XmlSchemaWalker;
import org.xml.sax.helpers.DefaultHandler;

import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.JsonProperties.NULL_VALUE;
import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

/**
 * XSD analyzer; can build an Avro schema corresponding to an XML schema, or a SAX {@link DefaultHandler} to parse it into an Avro record.
 */
public class XsdAnalyzer {
	public static final Set<String> XML_SCHEMA_DEFINITION_NAMESPACES = Set.of(Constants.URI_2001_SCHEMA_XSD, Constants.URI_2001_SCHEMA_XSI,
			Constants.XML_NS_URI, Constants.XMLNS_ATTRIBUTE_NS_URI, Constants.NULL_NS_URI);

	private final Map<String, String> namespaces;
	private final XmlSchema schema;

	/**
	 * Create an XSD analyzer.
	 *
	 * @param schemaUrl location of the main schema
	 */
	public XsdAnalyzer(URL schemaUrl) throws IOException {
		XmlSchemaCollection schemaCollection = new XmlSchemaCollection();
		String schemaLocation = schemaUrl.toExternalForm();
		try (InputStream inputStream = schemaUrl.openStream()) {
			schema = schemaCollection.read(new StreamSource(inputStream, schemaLocation));
		}

		namespaces = new LinkedHashMap<>();
		namespaces.put(schema.getLogicalTargetNamespace(), "ns"); // Ensure the target namespace is the first
		for (String xmlNamespace : availableNamespaces()) {
			NamespacePrefixList namespaceContext = schema.getNamespaceContext();
			for (String prefix : namespaceContext.getDeclaredPrefixes()) {
				String namespaceURI = namespaceContext.getNamespaceURI(prefix);
				if (!XML_SCHEMA_DEFINITION_NAMESPACES.contains(namespaceURI)) {
					String hash = new BigInteger(Utils.digest("MD5").digest(xmlNamespace.getBytes(UTF_8))).toString(16);
					namespaces.put(namespaceURI, "ns_" + hash);
				}
			}
		}
		namespaces.put(schema.getLogicalTargetNamespace(), "ns"); // Ensure the target namespace does not have a "hash name"
	}

	/**
	 * Returns the set of XML namespaces (URIs) defined in the XML schema. Does not return namespaces related to defining XML schemas.
	 *
	 * @return the set of XML namespaces in the XSD; the iterator returns the target namespace first
	 * @see #XML_SCHEMA_DEFINITION_NAMESPACES
	 */
	public Set<String> availableNamespaces() {
		return unmodifiableSet(namespaces.keySet());
	}

	/**
	 * Map the target XML namespace to an Avro namespace.
	 *
	 * @param avroNamespace the Avro namespace to map the target XML namespace to
	 */
	public void mapTargetNamespace(String avroNamespace) {
		mapNamespace(schema.getLogicalTargetNamespace(), avroNamespace);
	}

	/**
	 * Map an XML namespace to an Avro namespace.
	 *
	 * <p>Attempts to map an unknown XML namespace result in a no-op.</p>
	 *
	 * @param xmlNamespace  an XML namespace, as returned by {@link #availableNamespaces()}
	 * @param avroNamespace the Avro namespace to map the XML namespace to
	 */
	public void mapNamespace(String xmlNamespace, String avroNamespace) {
		namespaces.computeIfPresent(xmlNamespace, (key, oldNs) -> requireNonNull(avroNamespace));
	}

	/**
	 * Create an Avro schema for the given root element using the avro namespace mapped to the target namespace.
	 *
	 * @param rootElement the name of a root element in the XSD
	 * @return an Avro schema capable of containing any valid XML of the given root element
	 */
	public Schema createAvroSchema(String rootElement) {
		return createAvroSchema(new QName(schema.getLogicalTargetNamespace(), rootElement));
	}

	/**
	 * Create an Avro schema for the given root element using the avro namespace mapped to the given XML namespace.
	 *
	 * @param rootElement the root element in the XSD (with namespace URI)
	 * @return an Avro schema capable of containing any valid XML of the given root element
	 */
	public Schema createAvroSchema(QName rootElement) {
		//if (!namespaces.containsKey(rootElement.getNamespaceURI())) {
		//	throw new IllegalArgumentException()
		//}
		//String xmlNamespace = rootElement.getNamespaceURI();
		//String namespace = requireNonNull(namespaces.get(requireNonNull(xmlNamespace)), "Unmapped XML namespace");

		// Schemas by full name. Note that these schemas need not be records!
		Map<String, Schema> definedSchemasByFullname = new HashMap<>();
		StructureBuilder<ElementFields, Schema> structureBuilder = new StructureBuilder<>() {
			@Override
			public ElementFields startElement(FieldData fieldData, TypeData typeData, List<FieldData> attributes) {
				// If a type is complete, it will not contain any elements. The next method call will be to endElement(...).

				TypeData fullTypeData = typeData.extend(fieldData);
				String className = fullTypeData.name();

				ElementFields fields;
				// If typeData.isMixed(), then the type must be complex and fieldData.simpleType() == null
				if (attributes.isEmpty() && fieldData.scalarType() != null) {
					// Scalar element without attributes: behave like attribute
					fields = new ElementFields(fieldData, fullTypeData);
				} else if (attributes.isEmpty() && typeData.shouldNotBeParsed()) {
					// Unparsed element without attributes: behave like attribute
					fields = ElementFields.forUnparsedContent();
				} else {
					// The element has attributes, elements, or both
					fields = new ElementFields(fullTypeData);
					for (FieldData attributeData : attributes) {
						Schema.Field field = createField(attributeData, typeData);
						fields.addAttributeField(field);
					}
					if (fieldData.scalarType() != null) {
						fields.setValueField(createField(fieldData.withName("value"), typeData));
					} else if (typeData.shouldNotBeParsed()) {
						fields.setValueField(createField(fieldData.cardinality(), "value", "Text content of mixed XML element",
								Schema.createUnion(Schema.create(NULL), Schema.create(STRING)), NULL_DEFAULT_VALUE));
					}
				}

				// TODO: Determine if type is a custom type
				if (isCustomType(typeData)) {
					definedSchemasByFullname.put(className, fields.recordSchema());
				}

				return fields;
			}

			@Override
			public Schema endElement(ElementFields elementFields, FieldData fieldData, TypeData typeData) {
				return elementFields.completeRecordSchema();
			}

			@Override
			public Schema repeatedElement(FieldData fieldData, TypeData typeData) {
				String className = typeData.extend(fieldData).name();
				return definedSchemasByFullname.get(className);
			}

			@Override
			public void element(ElementFields parentElementState, FieldData fieldData, TypeData typeData, Schema elementResult) {
				if (parentElementState.isScalarValue()) {
					return;
				}

				Schema.Field newField;
				if (elementResult.getType() != RECORD) {
					newField = createField(fieldData, typeData);
				} else {
					List<Schema.Field> fields = elementResult.hasFields() ? elementResult.getFields() : emptyList();
					newField = createField(fieldData.cardinality(), fieldData.name(), fieldData.doc(), elementResult, null);
				}
				parentElementState.addElementField(newField);
			}

			@Override
			public void elementContainsAny(ElementFields parentElementState) {
				parentElementState.shouldNotParseElements(true);
			}
		};
		return walkSchema(rootElement, structureBuilder);
	}

	private static boolean isCustomType(TypeData typeData) {
		// TODO: Fix
		//return typeData.name() == null || !Constants.URI_2001_SCHEMA_XSD.equals(typeData.name().getNamespaceURI());
		return typeData.name() == null;
	}

	protected <Result> Result walkSchema(QName rootElement, StructureBuilder<?, Result> structureBuilder) {
		StructuralSchemaVisitor<?, Result> visitor = new StructuralSchemaVisitor<>(structureBuilder, namespaces::get, Integer.MAX_VALUE);
		walkSchema(rootElement, visitor, ScalarType.userRecognizedTypes());
		Result result = visitor.result();
		if (result == null) {
			throw new IllegalArgumentException("No schema: was the element abstract?");
		}
		return result;
	}

	// For testing
	void walkSchemaInTargetNamespace(@SuppressWarnings("SameParameterValue") String rootElement, XmlSchemaVisitor visitor, Set<QName> userRecognizedTypes) {
		walkSchema(new QName(schema.getLogicalTargetNamespace(), rootElement), visitor, userRecognizedTypes);
	}

	private void walkSchema(QName rootElement, XmlSchemaVisitor visitor, Set<QName> userRecognizedTypes) {
		XmlSchemaElement element = findRootElement(rootElement);

		XmlSchemaWalker schemaWalker = new XmlSchemaWalker(schema.getParent(), visitor);
		schemaWalker.setUserRecognizedTypes(userRecognizedTypes);
		schemaWalker.walk(element);
	}

	XmlSchemaElement findRootElement(QName rootElement) {
		XmlSchemaElement element = this.schema.getElementByName(rootElement);
		if (element == null) {
			throw new IllegalArgumentException("There is no root element " + rootElement + " defined in the XSD");
		}
		return element;
	}

	private static String fieldName(String name) {
		return Utils.snakeToLowerCamelCase(name);
	}

	private static String className(String name) {
		return Utils.snakeToUpperCamelCase(name);
	}

	protected static Schema.Field recreateField(Cardinality fieldCardinality, String name, String doc, Schema.Field oldField) {
		String newDoc = doc == null ? oldField.doc() : doc;
		return createField(fieldCardinality, name, newDoc, oldField.schema(), oldField.defaultVal());
	}

	protected static Schema.Field createField(FieldData fieldData, TypeData typeData) {
		final Schema schema;
		final Object defaultValue;
		if (fieldData.scalarType() instanceof FixedType fixedType) {
			schema = switch (fixedType) {
				case BOOLEAN -> Schema.create(BOOLEAN);
				case FLOAT -> Schema.create(FLOAT);
				case DOUBLE -> Schema.create(DOUBLE);
				case DATE -> LogicalTypes.date().addToSchema(Schema.create(INT));
				case DATETIME -> LogicalTypes.timestampMillis().addToSchema(Schema.create(LONG));
				case TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(INT));
				case BINARY_HEX, BINARY_BASE64 -> Schema.create(BYTES);
				default /* STRING */ -> Schema.create(STRING);
			};
			defaultValue = Optional.ofNullable(fieldData.defaultValue()).map(def -> switch (fixedType) {
				case BOOLEAN -> Boolean.parseBoolean(def);
				case FLOAT -> Float.parseFloat(def);
				case DOUBLE -> Double.parseDouble(def);
				case DATE -> (int) LocalDate.parse(def).toEpochDay();
				case DATETIME -> Instant.parse(def).toEpochMilli();
				case TIME -> {
					LocalTime localTime = LocalTime.parse(def);
					yield (int) (localTime.toNanoOfDay() / 1_000_000);
				}
				case BINARY_HEX -> new BigInteger(def, 16).toByteArray();
				case BINARY_BASE64 -> Base64.getDecoder().decode(def);
				default /* STRING */ -> def;
			}).orElse(null);
		} else if (fieldData.scalarType() instanceof EnumType enumType) {
			TypeData fullTypeData = typeData.extend(fieldData);
			schema = Schema.createEnum(fullTypeData.name(), fullTypeData.doc(), null, enumType.enumSymbols());
			defaultValue = fieldData.defaultValue();
			if (defaultValue != null && enumType.enumSymbols().contains(defaultValue)) {
				throw new ResolvingFailure("Unknown/illegal default enum value.");
			}
		} else if (fieldData.scalarType() instanceof DecimalType decimalType) {
			if (decimalType.scale() == 0 && decimalType.bitSize() <= Integer.SIZE) {
				schema = Schema.create(INT);
				defaultValue = Optional.ofNullable(fieldData.defaultValue()).map(Integer::decode).orElse(null);
			} else if (decimalType.scale() == 0 && decimalType.bitSize() <= Long.SIZE) {
				schema = Schema.create(LONG);
				defaultValue = Optional.ofNullable(fieldData.defaultValue()).map(Long::decode).orElse(null);
			} else {
				schema = LogicalTypes.decimal(decimalType.precision(), decimalType.scale()).addToSchema(Schema.create(BYTES));
				if (fieldData.defaultValue() != null) {
					BigDecimal decimal = new BigDecimal(fieldData.defaultValue());
					if (decimal.scale() > decimalType.scale() || decimal.precision() > decimalType.precision()) {
						throw new ResolvingFailure("Illegal decimal value: scale or precision too large.");
					}
					defaultValue = decimal.setScale(decimalType.scale(), UNNECESSARY);
				} else {
					defaultValue = null;
				}
			}
		} else {
			// scalarType == null
			schema = null;
			defaultValue = null;
		}

		return createField(fieldData.cardinality(), fieldData.name(), fieldData.doc(), schema, defaultValue);
	}

	protected static Schema.Field createField(Cardinality fieldCardinality, String name, String doc, Schema schema, Object defaultValue) {
		Cardinality schemaCardinality;
		Schema fieldSchema;
		if (schema.getType() == ARRAY) {
			schemaCardinality = Cardinality.MULTIPLE;
			fieldSchema = schema.getElementType();
		} else if (schema.getType() == UNION) {
			schemaCardinality = Cardinality.OPTIONAL;
			// We only ever create unions of null with a type and a default value of null
			fieldSchema = schema.getTypes().get(1);
		} else {
			schemaCardinality = Cardinality.REQUIRED;
			fieldSchema = schema;
		}
		Cardinality cardinality = fieldCardinality.adjustFor(schemaCardinality);
		if (cardinality == Cardinality.MULTIPLE && defaultValue != null) {
			defaultValue = null;
			//throw new ResolvingFailure("Default values are not supported for repeated elements.");
		}
		// Note: fields created by this class never have properties.
		return switch (cardinality) {
			case MULTIPLE -> new Schema.Field(name, Schema.createArray(fieldSchema), doc, emptyList());
			case OPTIONAL -> {
				Object newDefaultValue = Utils.first(defaultValue, NULL_DEFAULT_VALUE);
				if (newDefaultValue == NULL_DEFAULT_VALUE || newDefaultValue == NULL_VALUE) {
					yield new Schema.Field(name, Schema.createUnion(Schema.create(NULL), fieldSchema), doc, NULL_DEFAULT_VALUE);
				} else {
					yield new Schema.Field(name, Schema.createUnion(fieldSchema, Schema.create(NULL)), doc, newDefaultValue);
				}
			}
			default -> {
				if (defaultValue == null) {
					yield new Schema.Field(name, fieldSchema, doc);
				} else {
					yield new Schema.Field(name, fieldSchema, doc, defaultValue);
				}
			}
		};
	}

	private static class ElementFields {
		private static final Schema UNPARSED_STRING_SCHEMA;

		static {
			UNPARSED_STRING_SCHEMA = Schema.create(STRING);
			UNPARSED_STRING_SCHEMA.addProp("xmlDoNotParse", true);
		}

		private final Schema recordSchema;
		private boolean shouldNotParseElements;
		private Schema.Field valueField;
		private final List<Schema.Field> attributeFields;
		private final List<Schema.Field> elementFields;

		public ElementFields(TypeData typeData) {
			recordSchema = Schema.createRecord(typeData.name(), typeData.doc(), null, false);
			this.shouldNotParseElements = typeData.shouldNotBeParsed();
			attributeFields = new ArrayList<>();
			elementFields = new ArrayList<>();
		}

		private ElementFields(Schema recordSchema) {
			this.recordSchema = recordSchema;
			shouldNotParseElements = true;
			attributeFields = new ArrayList<>();
			elementFields = new ArrayList<>();
		}

		public ElementFields(FieldData fieldData, TypeData typeData) {
			this(createField(fieldData, typeData).schema());
		}

		public static ElementFields forUnparsedContent() {
			return new ElementFields(UNPARSED_STRING_SCHEMA);
		}

		public void shouldNotParseElements(boolean shouldNotParseElements) {
			this.shouldNotParseElements = shouldNotParseElements;
		}

		public boolean isScalarValue() {
			Schema.Type recordType;
			return recordSchema.getType() == null;
		}

		public void setValueField(Schema.Field valueField) {
			this.valueField = valueField;
		}

		public void addAttributeField(Schema.Field field) {
			attributeFields.add(field);
		}

		public void addElementField(Schema.Field field) {
			elementFields.add(field);
		}

		public List<Schema.Field> fields() {
			Stream<Schema.Field> elementFieldStream;
			if (valueField != null) {
				// The element has simple content, optionally with attributes
				elementFieldStream = Stream.of(valueField);
			} else if (shouldNotParseElements) {
				elementFieldStream = Stream.of(new Schema.Field("value", Schema.create(STRING), "The entire element content, unparsed."));
			} else {
				// The element contains fields
				elementFieldStream = elementFields.stream();
			}
			return Stream.concat(attributeFields.stream(), elementFieldStream).toList();
		}

		public Schema recordSchema() {
			return recordSchema;
		}

		public Schema completeRecordSchema() {
			// Note: MUST only be called once!
			if (recordSchema.getType() == RECORD) {
				recordSchema.setFields(fields());
			}
			return recordSchema;
		}
	}
}
