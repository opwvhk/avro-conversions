package opwvhk.avro.xml;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * A simplification of the SAX {@link ContentHandler} interface, with the added option to skip parsing of an element.
 *
 * <p>When the content of an element is "not parsed", it is actually reassembled, and character data in the element will be escaped to allow the result to be
 * handed to an XML parser. Note though, that the content is returned as-is: no attempt is made to reconstruct namespace declarations.</p>
 *
 * <p>The methods of this interface will be called in a fixed order:</p>
 *
 * <ol>
 *     <li>{@link #startDocument}</li>
 *     <li>{@link #startElement}</li>
 *     <li>Element content</li>
 *     <li>{@link #endElement}</li>
 *     <li>{@link #endDocument}</li>
 * </ol>
 *
 * <p>The element content yields either any number of calls to {@link #characters}, or any number of repetitions of items 2 to 4 above (this is recursive).</p>
 */
public interface SimpleContentHandler {
	/**
	 * Called at the beginning of a document. This is the first method to be invoked.
	 *
	 * @throws SAXException any SAX exception, possibly wrapping another exception
	 * @see #endDocument
	 */
	void startDocument() throws SAXException;

	/**
	 * Called at the end of a document, if parsing was successful. When this method returns, the caller will pass control to the application.
	 *
	 * @throws SAXException any SAX exception, possibly wrapping another exception
	 * @apiNote if parsing is not successful, the parser may abort with a {@link SAXException} and this method will never be called.
	 * @see #startDocument
	 */
	void endDocument() throws SAXException;

	/**
	 * Called whenever a new element starts.
	 *
	 * <p>For every invocation of this method, there will be a corresponding call to {@link #endElement}. All element content will be reported before the
	 * corresponding endElement call.</p>
	 *
	 * <p>The attribute list will not contain attributes used for namespace declarations (xmlns* attributes).</p>
	 *
	 * <p>Like {@link #characters characters()}, attribute values may have characters that need more than one <code>char</code> value.</p>
	 *
	 * @param uri       the namespace URI, or the empty string if the element has no namespace URI
	 * @param localName the local name (without prefix)
	 * @param qName     the qualified name (with prefix)
	 * @param attrs     the attributes attached to the element; can be an empty object (note that the object may be reused)
	 * @return {@code true} if the content of the element should be parsed, {@code false} if the content of the element should be passed as characters
	 * @throws SAXException any SAX exception, possibly wrapping another exception
	 * @see #endElement
	 */
	boolean startElement(String uri, String localName, String qName, Attributes attrs) throws SAXException;

	/**
	 * Called whenever a new element starts.
	 *
	 * <p>For every invocation of this method, there will be a corresponding call to {@link #startElement}.</p>
	 *
	 * @param uri       the namespace URI, or the empty string if the element has no namespace URI
	 * @param localName the local name (without prefix)
	 * @param qName     the qualified name (with prefix)
	 * @throws SAXException any SAX exception, possibly wrapping another exception
	 * @see #startElement
	 */
	void endElement(String uri, String localName, String qName) throws SAXException;

	/**
	 * Called whenever there is character data.
	 *
	 * <p>This method is called for every chunk of character data. Element content can be given in any number of chunks.</p>
	 *
	 * <p>Note that the argument may contain partial codepoints! Only when you combine the chunks from subsequent calls (without calls to other methods of this
	 * interface) can you be certain all code points are complete.</p>
	 *
	 * @param chars the characters from the XML document
	 * @throws SAXException any SAX exception, possibly wrapping another exception
	 */
	void characters(CharSequence chars) throws SAXException;
}
