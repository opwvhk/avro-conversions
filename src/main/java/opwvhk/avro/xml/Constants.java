package opwvhk.avro.xml;

import java.util.Set;

import static org.apache.ws.commons.schema.constants.Constants.URI_2001_SCHEMA_XSD;
import static org.apache.ws.commons.schema.constants.Constants.URI_2001_SCHEMA_XSI;
import static org.apache.ws.commons.schema.constants.Constants.XMLNS_ATTRIBUTE_NS_URI;
import static org.apache.ws.commons.schema.constants.Constants.XML_NS_URI;

final class Constants {
	public static final Set<String> XML_SCHEMA_DEFINITION_NAMESPACES = Set.of(URI_2001_SCHEMA_XSD, URI_2001_SCHEMA_XSI, XML_NS_URI, XMLNS_ATTRIBUTE_NS_URI);

	private Constants() {
		// Utility class: do not instantiate
	}
}
