package opwvhk.avro;

import java.io.IOException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

import opwvhk.avro.io.ValueResolver;
import opwvhk.avro.io.XmlAsAvroParser;
import org.xml.sax.SAXException;

public class AvroTest {
	public static void main(String[] args)
			throws IOException, SAXException {
		URL payloadXsd = ClassLoader.getSystemResource("parsing/payload.xsd");
		ValueResolver resolver = new DebugHandler("root");
		XmlAsAvroParser parser = new XmlAsAvroParser(payloadXsd, resolver);

		System.out.println("Parsing XML data 1:");
		System.out.printf("Parse result 1: %s\n", parser.parse(ClassLoader.getSystemResource("parsing/testdata1.xml")).toString());
		System.out.println();
		System.out.println("Parsing XML data 2:");
		System.out.printf("Parse result 2: %s\n", parser.parse(ClassLoader.getSystemResource("parsing/testdata2.xml")).toString());
		System.out.println();
		System.out.println("Parsing XML data 3:");
		System.out.printf("Parse result 3: %s\n", parser.parse(ClassLoader.getSystemResource("parsing/testdata3.xml")).toString());
	}

	static class DebugHandler
			extends ValueResolver {
		private final String myName;

		public DebugHandler(String myName) {
			this.myName = myName;
		}

		@Override
		public ValueResolver resolve(String name) {
			System.out.printf("%s has %s\n".formatted(myName, name));
			return new DebugHandler(myName + "." + name);
		}

		@Override
		public Map<String, Object> createCollector() {
			return new LinkedHashMap<>();
		}

		@Override
		public Object addProperty(Object collector, String name, Object value) {
			System.out.printf("%s.%s = %s\n".formatted(myName, name, value));
			//noinspection unchecked
			((Map<String, Object>)collector).put(name, value);
			return collector;
		}

		@Override
		public Object addContent(Object collector, String value) {
			System.out.printf("(%s = %s)\n".formatted(myName, value));
			//noinspection unchecked
			((Map<String, Object>)collector).put("", value);
			return collector;
		}

		@Override
		public Object complete(Object collector) {
			System.out.printf("end of %s\n", myName);
			//noinspection unchecked
			Map<String, Object> map = (Map<String, Object>) collector;
			if (map.size() == 1 && map.containsKey("")) {
				return map.get("");
			}
			return collector;
		}
	}
}
