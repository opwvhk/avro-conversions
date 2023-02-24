package opwvhk.avro.xsd;

import java.util.Set;

public final class Constants {
	static final Set<String> XML_SCHEMA_DEFINITION_NAMESPACES = Set.of(
			org.apache.ws.commons.schema.constants.Constants.URI_2001_SCHEMA_XSD, org.apache.ws.commons.schema.constants.Constants.URI_2001_SCHEMA_XSI,
			org.apache.ws.commons.schema.constants.Constants.XML_NS_URI, org.apache.ws.commons.schema.constants.Constants.XMLNS_ATTRIBUTE_NS_URI);

	private Constants() {
		// Utility class: do not instantiate
	}
}
