package opwvhk.avro.xsd;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import opwvhk.avro.datamodel.Cardinality;
import opwvhk.avro.structure.FieldData;
import opwvhk.avro.datamodel.FixedType;
import opwvhk.avro.datamodel.ScalarType;
import opwvhk.avro.datamodel.StructType;
import opwvhk.avro.structure.StructureBuilder;
import opwvhk.avro.datamodel.Type;
import opwvhk.avro.datamodel.TypeCollection;
import opwvhk.avro.structure.TypeData;
import opwvhk.avro.util.Utils;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.utils.NamespacePrefixList;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.apache.ws.commons.schema.walker.XmlSchemaWalker;
import org.xml.sax.helpers.DefaultHandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.ws.commons.schema.constants.Constants.*;

/**
 * XSD analyzer; can build an Avro schema corresponding to an XML schema, or a SAX {@link DefaultHandler} to parse it into an Avro record.
 */
public class XsdAnalyzer {
	public static final Set<QName> USER_RECOGNIZED_TYPES = Set.of(XSD_BOOLEAN, XSD_FLOAT, XSD_DOUBLE, XSD_DATE, XSD_DATETIME, XSD_TIME, XSD_INT, XSD_LONG,
			XSD_DECIMAL, XSD_STRING, XSD_ANYURI, XSD_HEXBIN, XSD_BASE64);

	private final XmlSchema schema;
	private final Map<String, String> namespaces;
	private final TypeCollection typeCollection;

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
				if (!opwvhk.avro.xsd.Constants.XML_SCHEMA_DEFINITION_NAMESPACES.contains(namespaceURI)) {
					String hash = new BigInteger(Utils.digest("MD5").digest(xmlNamespace.getBytes(UTF_8))).toString(16);
					namespaces.put(namespaceURI, "ns_" + hash);
				}
			}
		}
		namespaces.put(schema.getLogicalTargetNamespace(), "ns"); // Ensure the target namespace does not have a "hash name"

		typeCollection = new TypeCollection();
	}

	/**
	 * Returns the set of XML namespaces (URIs) defined in the XML schema. Does not return namespaces related to defining XML schemas.
	 *
	 * @return the set of XML namespaces in the XSD; the iterator returns the target namespace first
	 * @see opwvhk.avro.xsd.Constants#XML_SCHEMA_DEFINITION_NAMESPACES
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
	public Type typeOf(String rootElement) {
		return typeOf(new QName(schema.getLogicalTargetNamespace(), rootElement));
	}

	/**
	 * Create an Avro schema for the given root element using the avro namespace mapped to the given XML namespace.
	 *
	 * @param rootElement the root element in the XSD (with namespace URI)
	 * @return an Avro schema capable of containing any valid XML of the given root element
	 */
	public Type typeOf(QName rootElement) {
		// Schemas by full name. Note that these schemas need not be records!
		Map<String, Type> definedSchemasByFullname = new HashMap<>();
		StructureBuilder<ElementFields, Type> structureBuilder = new StructureBuilder<>() {
			@Override
			public ElementFields startElement(FieldData fieldData, TypeData typeData, List<FieldData> attributes) {
				// If a type is complete, it will not contain any elements. The next method call will be to endElement(...).

				String className = typeData.name();

				ElementFields fields;
				// If typeData.isMixed(), then the type must be complex and fieldData.simpleType() == null
				if (attributes.isEmpty() && fieldData.scalarType() != null) {
					// Scalar element without attributes: behave like attribute
					fields = new ElementFields(fieldData.scalarType());
				} else if (attributes.isEmpty() && typeData.shouldNotBeParsed()) {
					// Unparsed element without attributes: behave like attribute
					fields = new ElementFields(FixedType.STRING);
				} else {
					// The element has attributes, elements, or both
					fields = new ElementFields(typeData);
					for (FieldData attributeData : attributes) {
						ScalarType scalarType = requireNonNull(attributeData.scalarType());
						fields.addAttributeField(attributeData.cardinality(), attributeData.name(), attributeData.doc(), scalarType,
								attributeData.defaultValue());
					}
					if (fieldData.scalarType() != null) {
						FieldData fieldData1 = fieldData.withName("value");
						ScalarType scalarType = requireNonNull(fieldData1.scalarType());
						fields.setValueField(fieldData1.cardinality(), fieldData1.name(), fieldData1.doc(), scalarType, fieldData1.defaultValue());
					}
				}

				if (className != null) {
					definedSchemasByFullname.put(className, fields.recordSchema());
				}

				return fields;
			}

			@Override
			public Type endElement(ElementFields elementFields, FieldData fieldData, TypeData typeData) {
				return elementFields.completeRecordSchema();
			}

			@Override
			public Type repeatedElement(FieldData fieldData, TypeData typeData) {
				String className = typeData.name();
				return definedSchemasByFullname.get(className);
			}

			@Override
			public void element(ElementFields parentElementState, FieldData fieldData, TypeData typeData, Type elementResult) {
				if (parentElementState.isScalarValue()) {
					return;
				}

				Object defaultValue = elementResult instanceof StructType ? null : fieldData.defaultValue();
				parentElementState.addElementField(fieldData.cardinality(), fieldData.name(), fieldData.doc(), elementResult, defaultValue);
			}

			@Override
			public void elementContainsAny(ElementFields parentElementState) {
				parentElementState.shouldNotParseElements();
			}
		};
		return walkSchema(rootElement, structureBuilder);
	}

	protected <Result> Result walkSchema(QName rootElement, StructureBuilder<?, Result> structureBuilder) {
		StructuralSchemaVisitor<?, Result> visitor = new StructuralSchemaVisitor<>(structureBuilder, namespaces::get, Integer.MAX_VALUE);
		walkSchema(rootElement, visitor);
		Result result = visitor.result();
		if (result == null) {
			throw new IllegalArgumentException("No schema: was the element abstract?");
		}
		return result;
	}

	// For testing
	void walkSchemaInTargetNamespace(@SuppressWarnings("SameParameterValue") String rootElement, XmlSchemaVisitor visitor) {
		walkSchema(new QName(schema.getLogicalTargetNamespace(), rootElement), visitor);
	}

	private void walkSchema(QName rootElement, XmlSchemaVisitor visitor) {
		XmlSchemaElement element = findRootElement(rootElement);

		XmlSchemaWalker schemaWalker = new XmlSchemaWalker(schema.getParent(), visitor);
		schemaWalker.setUserRecognizedTypes(XsdAnalyzer.USER_RECOGNIZED_TYPES);
		schemaWalker.walk(element);
	}

	XmlSchemaElement findRootElement(QName rootElement) {
		XmlSchemaElement element = this.schema.getElementByName(rootElement);
		if (element == null) {
			throw new IllegalArgumentException("There is no root element " + rootElement + " defined in the XSD");
		}
		return element;
	}

	private class ElementFields {
		private final Type recordType;
		private boolean shouldNotParseElements;
		private StructType.Field valueField;
		private final List<StructType.Field> attributeFields;
		private final List<StructType.Field> elementFields;

		public ElementFields(TypeData typeData) {
			recordType = new StructType(typeCollection, typeData.name(), typeData.doc());
			this.shouldNotParseElements = typeData.shouldNotBeParsed();
			attributeFields = new ArrayList<>();
			elementFields = new ArrayList<>();
		}

		private ElementFields(Type recordType) {
			this.recordType = recordType;
			shouldNotParseElements = true;
			attributeFields = new ArrayList<>();
			elementFields = new ArrayList<>();
		}

		public void shouldNotParseElements() {
			this.shouldNotParseElements = true;
		}

		public boolean isScalarValue() {
			return recordType instanceof ScalarType;
		}

		public void setValueField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
			this.valueField = createField(fieldCardinality, name, doc, type, defaultValue);
		}

		public void addAttributeField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
			attributeFields.add(createField(fieldCardinality, name, doc, type, defaultValue));
		}

		public void addElementField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
			elementFields.add(createField(fieldCardinality, name, doc, type, defaultValue));
		}

		private StructType.Field createField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
			return new StructType.Field(name, doc, fieldCardinality, type, defaultValue);
		}

		public List<StructType.Field> fields() {
			Stream<StructType.Field> elementFieldStream;
			if (shouldNotParseElements) {
				elementFieldStream = Stream.of(new StructType.Field("value", "The entire element content, unparsed.", Cardinality.OPTIONAL, FixedType.STRING,
						StructType.Field.NULL_VALUE));
			} else if (valueField != null) {
				// The element has simple content, optionally with attributes
				elementFieldStream = Stream.of(valueField);
			} else {
				// The element contains fields
				elementFieldStream = elementFields.stream();
			}
			return Stream.concat(attributeFields.stream(), elementFieldStream).toList();
		}

		public Type recordSchema() {
			return recordType;
		}

		public Type completeRecordSchema() {
			// Note: MUST only be called once!
			if (recordType instanceof StructType structType) {
				structType.setFields(fields());
			}
			return recordType;
		}
	}
}
