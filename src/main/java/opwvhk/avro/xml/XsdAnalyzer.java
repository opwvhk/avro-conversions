package opwvhk.avro.xml;

import opwvhk.avro.util.Utils;
import opwvhk.avro.xml.datamodel.Type;
import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.utils.NamespacePrefixList;
import org.apache.ws.commons.schema.walker.FixedXmlSchemaWalker;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.ws.commons.schema.constants.Constants.XSD_ANYURI;
import static org.apache.ws.commons.schema.constants.Constants.XSD_BASE64;
import static org.apache.ws.commons.schema.constants.Constants.XSD_BOOLEAN;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DATE;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DATETIME;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DECIMAL;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DOUBLE;
import static org.apache.ws.commons.schema.constants.Constants.XSD_FLOAT;
import static org.apache.ws.commons.schema.constants.Constants.XSD_HEXBIN;
import static org.apache.ws.commons.schema.constants.Constants.XSD_INT;
import static org.apache.ws.commons.schema.constants.Constants.XSD_LONG;
import static org.apache.ws.commons.schema.constants.Constants.XSD_STRING;
import static org.apache.ws.commons.schema.constants.Constants.XSD_TIME;

/**
 * XSD analyzer; can build an Avro schema corresponding to an XML schema, or a SAX {@link DefaultHandler} to parse it into an Avro record.
 */
public class XsdAnalyzer {
	private static final Set<QName> USER_RECOGNIZED_TYPES = Set.of(XSD_BOOLEAN, XSD_FLOAT, XSD_DOUBLE, XSD_DATE, XSD_DATETIME, XSD_TIME, XSD_INT, XSD_LONG,
			XSD_DECIMAL, XSD_STRING, XSD_ANYURI, XSD_HEXBIN, XSD_BASE64);

	private final XmlSchema schema;
	private final Map<String, String> namespaces;

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
				if (!Constants.XML_SCHEMA_DEFINITION_NAMESPACES.contains(namespaceURI)) {
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
	 * @see Constants#XML_SCHEMA_DEFINITION_NAMESPACES
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
	public Schema schemaOf(String rootElement) {
		return typeOf(rootElement).toSchema();
	}

	/**
	 * Create a type description for the given root element using the avro namespace mapped to the target namespace.
	 *
	 * @param rootElement the name of a root element in the XSD
	 * @return a descriptor describing the XML schema for use as an Object
	 */
	Type typeOf(String rootElement) {
		return typeOf(new QName(schema.getLogicalTargetNamespace(), rootElement));
	}

	/**
	 * Create a type description for the given root element using the avro namespace mapped to the given XML namespace.
	 *
	 * @param rootElement the root element in the XSD (with namespace URI)
	 * @return a descriptor describing the XML schema for use as an Object
	 */
	Type typeOf(QName rootElement) {
		TypeBuildingVisitor visitor = new TypeBuildingVisitor(new TypeStructureBuilder(), namespaces::get, Integer.MAX_VALUE);
		walkSchema(rootElement, visitor);
		Type result = visitor.result();
		if (result == null) {
			throw new IllegalArgumentException("No schema: was the element abstract?");
		}
		return result;
	}

	// For testing
	void walkSchemaInTargetNamespace(String rootElement, XmlSchemaVisitor visitor) {
		walkSchema(new QName(schema.getLogicalTargetNamespace(), rootElement), visitor);
	}

	private void walkSchema(QName rootElement, XmlSchemaVisitor visitor) {
		XmlSchemaElement element = findRootElement(rootElement);

		// TODO: Replace with the original XmlSchemaWalker, after a fix for XMLSCHEMA-64 is released.
		FixedXmlSchemaWalker schemaWalker = new FixedXmlSchemaWalker(schema.getParent(), visitor);
		schemaWalker.setUserRecognizedTypes(XsdAnalyzer.USER_RECOGNIZED_TYPES);
		schemaWalker.walk(element);
	}

	private XmlSchemaElement findRootElement(QName rootElement) {
		XmlSchemaElement element = this.schema.getElementByName(rootElement);
		if (element == null) {
			throw new IllegalArgumentException("There is no root element " + rootElement + " defined in the XSD");
		}
		return element;
	}
}
