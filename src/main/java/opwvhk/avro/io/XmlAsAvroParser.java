package opwvhk.avro.io;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.Set;

import opwvhk.avro.XsdAnalyzer;
import opwvhk.avro.xml.SimpleContentAdapter;
import opwvhk.avro.xml.XmlRecordHandler;
import org.apache.avro.generic.GenericData;
import org.apache.ws.commons.schema.constants.Constants;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XmlAsAvroParser {
	public static final Set<String> XML_SCHEMA_DEFINITION_NAMESPACES = Set.of(Constants.URI_2001_SCHEMA_XSD, Constants.URI_2001_SCHEMA_XSI,
			Constants.XML_NS_URI, Constants.XMLNS_ATTRIBUTE_NS_URI, Constants.NULL_NS_URI);
	private final GenericData model;
	private final SAXParser parser;
	private final ValueResolver resolver;

	public static void main(String[] args) throws IOException, SAXException {
		URL xsdLocation = ClassLoader.getSystemResource("parsing/payload.xsd");
		org.apache.avro.Schema readSchema = null;
		DefaultData model = new DefaultData(GenericData.get());
		XmlAsAvroParser xmlAsAvroParser = new XmlAsAvroParser(xsdLocation, "Envelope", readSchema, model);
		System.out.printf("XML data 1:\n%s\n", xmlAsAvroParser.parse(ClassLoader.getSystemResource("parsing/testdata1.xml")));
		//System.out.printf("XML data 2:\n%s\n", xmlAsAvroParser.parse(ClassLoader.getSystemResource("parsing/testdata2.xml")));
		//System.out.printf("XML data 3:\n%s\n", xmlAsAvroParser.parse(ClassLoader.getSystemResource("parsing/testdata3.xml")));
	}

	public XmlAsAvroParser(URL xsdLocation, String rootElement, org.apache.avro.Schema readSchema, GenericData model) throws IOException {
		this.model = model;
		parser = createParser(xsdLocation);

		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(xsdLocation);
		org.apache.avro.Schema writeSchema = xsdAnalyzer.createAvroSchema(rootElement);

		resolver = resolve(readSchema, writeSchema);
	}

	public XmlAsAvroParser(URL xsdLocation, ValueResolver resolver) {
		this.model = GenericData.get();
		parser = createParser(xsdLocation);
		this.resolver = resolver;
	}

	private SAXParser createParser(URL xsdLocation) {
		try {
			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			parserFactory.setNamespaceAware(true);

			SchemaFactory schemaFactory = SchemaFactory.newDefaultInstance();
			Schema schema = schemaFactory.newSchema(xsdLocation);
			parserFactory.setSchema(schema);

			return parserFactory.newSAXParser();
		} catch (SAXException | ParserConfigurationException e) {
			throw new IllegalStateException("Failed to create parser", e);
		}
	}

	private ValueResolver resolve(org.apache.avro.Schema readSchema, org.apache.avro.Schema writeSchema) {
		// TODO: Implement
		return null;
	}

	protected <T> T parse(InputSource source) throws IOException, SAXException {
		XmlRecordHandler handler = new XmlRecordHandler(resolver);
		parser.parse(source, new SimpleContentAdapter(handler));
		return handler.getValue();
	}

	public <T> T parse(URL url) throws IOException, SAXException {
		return parse(urlAsXmlSource(url));
	}

	private InputSource urlAsXmlSource(URL url) {
		InputSource inputSource = new InputSource();
		inputSource.setSystemId(url.toExternalForm());
		return inputSource;
	}

	@SuppressWarnings("unused")
	public <T> T parse(String xmlData) throws IOException, SAXException {
		InputSource inputSource = stringAsXmlSource(xmlData);
		return parse(inputSource);
	}

	private InputSource stringAsXmlSource(String xmlData) {
		return new InputSource(new StringReader(xmlData));
	}
}
