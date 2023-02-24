package opwvhk.avro.xml;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

import opwvhk.avro.xsd.XsdAnalyzer;
import opwvhk.avro.io.DefaultData;
import opwvhk.avro.io.ValueResolver;
import opwvhk.avro.datamodel.Type;
import opwvhk.avro.datamodel.TypeCollection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XmlAsAvroParser {
	private final SAXParser parser;
	private final ValueResolver resolver;

	public static void main(String[] args) throws IOException, SAXException {
		URL xsdLocation = ClassLoader.getSystemResource("opwvhk/avro/xml/payload.xsd");
		org.apache.avro.Schema readSchema = null;
		DefaultData model = new DefaultData(GenericData.get());
		XmlAsAvroParser xmlAsAvroParser = new XmlAsAvroParser(xsdLocation, "Envelope", readSchema, model);
		System.out.printf("XML data 1:\n%s\n", xmlAsAvroParser.parse(ClassLoader.getSystemResource("opwvhk/avro/xml/testdata1.xml")));
		//System.out.printf("XML data 2:\n%s\n", xmlAsAvroParser.parse(ClassLoader.getSystemResource("parsing/testdata2.xml")));
		//System.out.printf("XML data 3:\n%s\n", xmlAsAvroParser.parse(ClassLoader.getSystemResource("parsing/testdata3.xml")));
	}

	public XmlAsAvroParser(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		parser = createParser(xsdLocation);
		resolver = createResolver(xsdLocation, rootElement, readSchema, model);
	}

	XmlAsAvroParser(URL xsdLocation, ValueResolver resolver) {
		parser = createParser(xsdLocation);
		this.resolver = resolver;
	}

	private SAXParser createParser(URL xsdLocation) {
		try {
			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			parserFactory.setNamespaceAware(true);

			SchemaFactory schemaFactory = SchemaFactory.newDefaultInstance();
			javax.xml.validation.Schema schema = schemaFactory.newSchema(xsdLocation);
			parserFactory.setSchema(schema);

			return parserFactory.newSAXParser();
		} catch (SAXException | ParserConfigurationException e) {
			throw new IllegalStateException("Failed to create parser", e);
		}
	}

	private ValueResolver createResolver(URL xsdLocation, String rootElement, Schema readSchema, GenericData model) throws IOException {
		Type writeType = determineWriteType(xsdLocation, rootElement);
		Type readType = determineReadType(readSchema);
		return resolve(readType, writeType, model);
	}

	private static Type determineReadType(Schema readSchema) {
		TypeCollection typeCollection = new TypeCollection();
		return Type.fromSchema(typeCollection, readSchema);
	}

	private static Type determineWriteType(URL xsdLocation, String rootElement) throws IOException {
		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(xsdLocation);
		return xsdAnalyzer.typeOf(rootElement);
	}

	private ValueResolver resolve(Type readType, Type writeType, GenericData model) {
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
