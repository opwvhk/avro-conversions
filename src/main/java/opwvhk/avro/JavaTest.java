package opwvhk.avro;

import org.apache.avro.compiler.idl.ParseException;

import java.io.IOException;
import java.util.Objects;

public class JavaTest {
	public static void main(String[] args) throws IOException, ParseException {
		//InputStream resource = JavaTest.class.getResourceAsStream("/schema.avsc");
		//Schema schema = new Schema.Parser().parse(resource);

		//StringWriter buffer = new StringWriter();
		//IdlUtils.writeIdlProtocol("opwvhk.weather", "Weather", buffer, schema);
		//System.out.println(buffer.toString());
		//System.out.println();

		//Idl idl = new Idl(new StringReader(buffer.toString()));
		//Protocol protocol = idl.CompilationUnit();
		//for (Schema type : protocol.getTypes()) {
		//	System.out.println(type.getFullName());
		//}
		//Optional<Schema> firstSchema = protocol.getTypes().stream().findFirst();
		//System.out.println(firstSchema.toString());
		//
		//System.out.println(protocol.toString(true));
		//System.out.println();
		//System.out.println();
		//System.out.println();
	}

	/*
	private static void addConfig(XsdAnalyzer xsdAnalyzer) {
		xsdAnalyzer.addNameOverride(".Payload.base64encoded", "Base64Encoded");
		xsdAnalyzer.addNameOverride(".Payload.messagetype", "MessageType");

		xsdAnalyzer.addClassToKeep("Departure");
	}
	*/
}
