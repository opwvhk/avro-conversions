package opwvhk.avro.xml;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import static java.util.Objects.requireNonNull;
import static opwvhk.avro.xml.Constants.XML_SCHEMA_DEFINITION_NAMESPACES;

/**
 * Adapter to treat a {@link SimpleContentHandler} as a SAX {@link org.xml.sax.ContentHandler}.
 *
 * <p>Requires the parser to support namespaces (and return namespace attributes).</p>
 */
class SimpleContentAdapter extends DefaultHandler {
	private static final int DEFAULT_BUFFER_CAPACITY = 1024;
	private final SimpleContentHandler simpleContentHandler;
	private final StringBuilder charBuffer;
	private int reassemblingDepth;
	private boolean reassemblingStartTag;

	SimpleContentAdapter(SimpleContentHandler simpleContentHandler) {
		this.simpleContentHandler = simpleContentHandler;
		charBuffer = new StringBuilder(DEFAULT_BUFFER_CAPACITY);
	}

	@Override
	public void startDocument() {
		charBuffer.setLength(0);
		reassemblingDepth = -1;
		reassemblingStartTag = false;

		simpleContentHandler.startDocument();
	}

	@Override
	public void endDocument() {
		simpleContentHandler.endDocument();
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) {
		if (reassemblingDepth < 0) {
			AttributesImpl filteredAttrs = new AttributesImpl();
			for (int i = 0; i < attributes.getLength(); i++) {
				String attrUri = attributes.getURI(i);
				if (!XML_SCHEMA_DEFINITION_NAMESPACES.contains(attrUri)) {
					filteredAttrs.addAttribute(attrUri, attributes.getLocalName(i), attributes.getQName(i), attributes.getType(i), attributes.getValue(i));
				}
			}
			if (!simpleContentHandler.startElement(uri, localName, qName, filteredAttrs)) {
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
				// Unfiltered: we're reassembling everything as-is
				String attrQName = requireNonNull(attributes.getQName(i));
				String attrValue = attributes.getValue(i).replace("\"", "&quot;'");
				charBuffer.append(" ").append(attrQName).append("=\"").append(attrValue).append("\"");
			}

			reassemblingDepth++;
			reassemblingStartTag = true;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) {
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
	public void characters(char[] ch, int start, int length) {
		String content = String.valueOf(ch, start, length);
		if (reassemblingDepth >= 0) {
			if (reassemblingStartTag) {
				charBuffer.append(">");
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
				charBuffer.append(">");
				reassemblingStartTag = false;
			}
			// Can be added as-is, because whitespace does not need escaping for XML parsers
			charBuffer.append(ch, start, length);
		}
	}

	@Override
	public void error(SAXParseException e) throws SAXException {
		throw e;
	}
}
