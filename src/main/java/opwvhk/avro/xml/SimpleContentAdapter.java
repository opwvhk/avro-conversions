package opwvhk.avro.xml;

import java.util.Set;

import org.apache.ws.commons.schema.constants.Constants;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import static java.util.Objects.requireNonNull;

/**
 * Adapter to treat a {@link SimpleContentHandler} as a SAX {@link org.xml.sax.ContentHandler}.
 *
 * <p>Requires the parser to support namespaces (and return namespace attributes).</p>
 */
public class SimpleContentAdapter extends DefaultHandler {
	private static final Set<String> XML_SCHEMA_DEFINITION_NAMESPACES = Set.of(Constants.URI_2001_SCHEMA_XSD, Constants.URI_2001_SCHEMA_XSI,
			Constants.XML_NS_URI, Constants.XMLNS_ATTRIBUTE_NS_URI, Constants.NULL_NS_URI);
	private static final int DEFAULT_BUFFER_CAPACITY = 1024;
	private final SimpleContentHandler simpleContentHandler;
	private final StringBuilder charBuffer;
	private int reassemblingDepth;
	private boolean reassemblingStartTag;

	public SimpleContentAdapter(SimpleContentHandler simpleContentHandler) {
		this.simpleContentHandler = simpleContentHandler;
		charBuffer = new StringBuilder(DEFAULT_BUFFER_CAPACITY);
	}

	@Override
	public void startDocument() throws SAXException {
		charBuffer.setLength(0);
		reassemblingDepth = -1;
		reassemblingStartTag = false;

		simpleContentHandler.startDocument();
	}

	@Override
	public void endDocument() throws SAXException {
		simpleContentHandler.endDocument();
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if (reassemblingDepth < 0) {
			AttributesImpl filteredAttrs = new AttributesImpl();
			for (int i = 0; i < attributes.getLength(); i++) {
				String attrUri = attributes.getURI(i);
				if (!XML_SCHEMA_DEFINITION_NAMESPACES.contains(attrUri)) {
					filteredAttrs.addAttribute(attrUri, attributes.getLocalName(i), attributes.getQName(i), attributes.getType(i), attributes.getValue(i));
				}
			}
			if (simpleContentHandler.startElement(uri, localName, qName, filteredAttrs)) {
				reassemblingDepth = 0;
				reassemblingStartTag = false;
			}
		} else {
			if (reassemblingStartTag) {
				charBuffer.append(">");
				reassemblingStartTag = false;
			}
			charBuffer.append("<").append(qName);
			for (int i = 0; i < attributes.getLength(); i++) {
				// Unfiltered: we're
				String attrQName = requireNonNull(attributes.getQName(i));
				String attrValue = attributes.getValue(i).replace("\"", "&quot;'");
				charBuffer.append(" ").append(attrQName).append("=\"").append(attrValue).append("\"");
			}

			reassemblingDepth++;
			reassemblingStartTag = true;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (reassemblingDepth > 0) {
			if (reassemblingStartTag) {
				charBuffer.append("/>");
				reassemblingStartTag = false;
			} else {
				charBuffer.append("</");
				charBuffer.append(qName);
				charBuffer.append(">");
			}
			reassemblingDepth--;
		} else {
			if (reassemblingDepth == 0) {
				simpleContentHandler.characters(charBuffer);
				charBuffer.setLength(0);
			}
			reassemblingDepth = -1;
			reassemblingStartTag = false;

			simpleContentHandler.endElement(uri, localName, qName);
		}
	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		String content = String.valueOf(ch, start, length);
		if (reassemblingDepth >= 0) {
			if (reassemblingStartTag) {
				charBuffer.append("/>");
				reassemblingStartTag = false;
			}
			charBuffer.append(escapeForXml(content));
		} else {
			simpleContentHandler.characters(content);
		}
	}

	private static String escapeForXml(String content) {
		return content
				.replace("&", "&amp;") // Must be first to prevent escaping the escapes
				.replace("<", "&lt;")
				.replace(">", "&gt;");
	}

	@Override
	public void ignorableWhitespace(char[] ch, int start, int length) {
		if (reassemblingDepth >= 0) {
			if (reassemblingStartTag) {
				charBuffer.append("/>");
				reassemblingStartTag = false;
			}
			// Can be added as-is, because whitespace does not need escaping for XML parsers
			charBuffer.append(ch, start, length);
		}
	}
}
