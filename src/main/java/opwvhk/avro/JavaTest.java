package opwvhk.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;

import opwvhk.avro.util.IdlUtils;
import opwvhk.avro.xml.XsdAnalyzer;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.specific.AvroGenerated;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("ALL")
@AvroGenerated // HACK: an annotation with "Generated" in the name causes JaCoCo to exclude this class/method from code coverage
public class JavaTest {
	public static void main(String[] args) throws Exception {
		URL resolvingTestXsd = requireNonNull(JavaTest.class.getResource("/opwvhk/avro/xml/resolvingTest.xsd"));
		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(resolvingTestXsd);
		xsdAnalyzer.mapTargetNamespace("opwvhk.resolvingTest");
		System.out.println(xsdAnalyzer.schemaOf("outer"));
	}

	/*
	private static void addConfig(XsdAnalyzer xsdAnalyzer) {
		xsdAnalyzer.addNameOverride(".Payload.base64encoded", "Base64Encoded");
		xsdAnalyzer.addNameOverride(".Payload.messagetype", "MessageType");

		xsdAnalyzer.addClassToKeep("Departure");
	}
	*/
}
