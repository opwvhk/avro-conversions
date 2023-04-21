package opwvhk.avro;

import java.net.URL;

import opwvhk.avro.xml.XsdAnalyzer;
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
