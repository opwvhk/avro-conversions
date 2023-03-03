package opwvhk.avro.xml;

import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BasicXmlParsingTest {
	/**
	 * Logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(BasicXmlParsingTest.class);

	private XmlAsAvroParser parser;
	private URL testdataTextPayload;
	private URL testdataBinaryPayload;
	private URL testdataXmlPayload;
	private URL testdataDefaultAndCompactXmlPayload;

	@Before
	public void setUp() {
		URL payloadXsd = requireNonNull(getClass().getResource("payload.xsd"));
		parser = new XmlAsAvroParser(payloadXsd.toExternalForm(), new PayloadDebugHandler());

		testdataTextPayload = getClass().getResource("textPayload.xml");
		testdataBinaryPayload = getClass().getResource("binaryPayload.xml");
		testdataXmlPayload = getClass().getResource("xmlPayload.xml");
		testdataDefaultAndCompactXmlPayload = getClass().getResource("defaultAndCompactXmlPayload.xml");
	}

	@Test
	public void textXsdIsRequired() {
		assertThatThrownBy(() -> new XmlAsAvroParser(null, new PayloadDebugHandler())).isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void testTextPayload() throws IOException, SAXException {
		assertThat(parser.<Map<String, Object>>parse(testdataTextPayload)).isEqualTo(Map.of(
				"source", "Bronsysteem",
				"target", "Bestemming",
				"payload", Map.of(
						"type", "text",
						"", "Hello World!"
				)
		));
	}

	@Test
	public void testRawBinaryPayload() throws IOException, SAXException {
		assertThat(parser.<Map<String, Object>>parse(testdataBinaryPayload)).isEqualTo(Map.of(
				"source", "Bronsysteem",
				"target", "Bestemming",
				"payload", Map.of(
						"type", "binary",
						"", "SGVsbG8gV29ybGQhCg==" // This resolver doesn't parse Base64
				)
		));
	}

	@Test
	public void testXmlPayload() throws IOException, SAXException {
		assertThat(parser.<Map<String, Object>>parse(testdataXmlPayload)).isEqualTo(Map.of(
				"source", "Bronsysteem",
				"target", "Bestemming",
				"payload", Map.of(
						"type", "xml",
						"", """
								<record>
									<title>Status Report</title>
									<status>OPEN</status>
									<sequence>
										<number>1</number>
										<number>1</number>
										<number>2</number>
										<number>3</number>
										<number>5</number>
										<number>8</number>
									</sequence>
									<nested>
										<description>A description of the strings</description>
										<strings>one</strings>
										<strings>two</strings>
										<strings>three</strings>
									</nested>
								</record>"""
				)
		));
	}

	@Test
	public void testDefaultPayload() throws IOException, SAXException {
		assertThat(parser.<Map<String, Object>>parse(testdataDefaultAndCompactXmlPayload)).isEqualTo(Map.of(
				"source", "Bronsysteem",
				"target", "Bestemming",
				"payload", Map.of(
						"type", "xml", // XSD default values are implicitly parsed (unlike DTD #IMPLIED values)
						// Note the empty line: comments cannot be reconstructed...
						"", """
								<record>

									<title>Status Report</title><summary language="NL">Een korte omschrijving</summary><status>OPEN</status><sequence/>
									<nested><description>No strings this time</description></nested>
								</record>"""
				)
		));
	}

	static class PayloadDebugHandler extends ValueResolver {
		private final String myName;
		private final String myPrefix;

		private PayloadDebugHandler(String myName, String myPrefix) {
			this.myName = myName;
			this.myPrefix = myPrefix;
		}

		private PayloadDebugHandler() {
			this("<root>", null);
		}

		@Override
		public ValueResolver resolve(String name) {
			LOGGER.debug("{} has {}", myName, name);
			String newName = myPrefix == null ? name : myPrefix + name;
			return new PayloadDebugHandler(newName, newName + ".");
		}

		@Override
		public Map<String, Object> createCollector() {
			return new LinkedHashMap<>();
		}

		@Override
		public Object addProperty(Object collector, String name, Object value) {
			LOGGER.debug("{}.{} = {}", myName, name, value);
			((Map<String, Object>) collector).put(name, value);
			return collector;
		}

		@Override
		public Object addContent(Object collector, String value) {
			LOGGER.debug("({} = {})", myName, value);
			((Map<String, Object>) collector).put("", value);
			return collector;
		}

		@Override
		public Object complete(Object collector) {
			LOGGER.debug("end of {}", myName);
			Map<String, Object> map = (Map<String, Object>) collector;
			if (map.size() == 1 && map.containsKey("")) {
				return map.get("");
			}
			return collector;
		}

		@Override
		public boolean parseContent() {
			return !"payload".equals(myName);
		}
	}
}
