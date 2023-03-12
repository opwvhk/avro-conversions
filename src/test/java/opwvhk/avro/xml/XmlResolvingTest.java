package opwvhk.avro.xml;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import opwvhk.avro.ResolvingFailure;
import opwvhk.avro.xml.datamodel.DecimalType;
import opwvhk.avro.xml.datamodel.FixedType;
import opwvhk.avro.xml.datamodel.Type;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static java.util.Objects.requireNonNull;
import static opwvhk.avro.xml.datamodel.TestStructures.array;
import static opwvhk.avro.xml.datamodel.TestStructures.enumType;
import static opwvhk.avro.xml.datamodel.TestStructures.optional;
import static opwvhk.avro.xml.datamodel.TestStructures.required;
import static opwvhk.avro.xml.datamodel.TestStructures.struct;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class XmlResolvingTest {

	private static final GenericData MODEL = GenericData.get();

	@BeforeClass
	public static void beforeClass() {
		Assertions.setMaxStackTraceElementsDisplayed(10);

		MODEL.addLogicalTypeConversion(new TimeConversions.DateConversion());
		MODEL.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
		MODEL.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
		MODEL.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
		MODEL.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
		MODEL.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
		MODEL.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
	}

	@Test
	public void testHappyFlowSchema() throws IOException {
		URL xsdLocation = requireNonNull(getClass().getResource("resolvingTest.xsd"));
		XsdAnalyzer xsdAnalyzer = new XsdAnalyzer(xsdLocation);
		Schema actualSchema = xsdAnalyzer.schemaOf("outer");

		Schema expectedSchema = new Schema.Parser().parse(getClass().getResourceAsStream("resolvingTestWriter.avsc"));
		assertThat(actualSchema).isEqualTo(expectedSchema);
	}

	@SuppressWarnings("UnnecessaryUnicodeEscape")
	@Test
	public void testSuccessfulResolvingAndParsing() throws IOException, SAXException {
		URL xsdLocation = requireNonNull(getClass().getResource("resolvingTest.xsd"));
		Schema readSchema = new Schema.Parser().parse(getClass().getResourceAsStream("resolvingTest.avsc"));
		XmlAsAvroParser parser = new XmlAsAvroParser(xsdLocation, "outer", readSchema, MODEL);

		GenericRecord resultFull = parser.parse(requireNonNull(getClass().getResource("resolvingTestFull.xml")));
		assertThat(toJson(resultFull)).isEqualToNormalizingWhitespace("""
				{
					"presentRequired" : "I'm here",
					"optionalField" : { "string" : "I'm here too" },
					"textList" : [ "Me too", "Hey, that's my line!" ],
					"inner" : { "opwvhk.resolvingTest.innerType" : {
						"e" : { "opwvhk.resolvingTest.e" : "three" },
						"amount" : {
							"currency" : { "string" : "EUR" },
							"value" : "\\u0007\u005B\u00CD\\f"
						},
						"b" : { "boolean" : true },
						"b64Bytes" : { "bytes" : "Hello World!\\n" },
						"d" : { "int" : 19432 },
						"dt" : { "long" : 1678974301123 },
						"dtu" : { "long" : 1678974301123456 },
						"fd" : { "double" : 123456.789012 },
						"fs" : { "float" : 123.456 },
						"hexBytes" : { "bytes" : "Hello World!\\n" },
						"s" : { "string" : "text" },
						"t" : { "int" : 49501123 },
						"tu" : { "long" : 49501123456 },
						"numberHuge" : "\\u0001\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000",
						"numberInt" : 93658723,
						"numberLong" : 2147483648,
						"numbers" : [ 1, 2, 4, 8, 16 ],
						"numberUnbounded" : 9223372036854775807
					} },
					"switch" : "broken",
					"approximation" : 123456.78,
					"moreAccurateApproximation" : { "double" : 1.23456789012345E8 },
					"morePrecise" : { "double" : 12.5 },
					"exceptionToUnwrappingRule" : { "opwvhk.resolvingTest.mustMatch" : {
						"number" : [ 1, 1, 2, 3, 5, 8 ]
					} },
					"upgrade" : [ {
						"key" : "single",
						"value" : "value"
					} ],
					"category" : { "string" :  "apple" }
				}
				""");

		GenericRecord resultMinimal = parser.parse(requireNonNull(getClass().getResource("resolvingTestMinimal.xml")));
		assertThat(toJson(resultMinimal)).isEqualToNormalizingWhitespace("""
				{
					"presentRequired" : "I'm here",
					"optionalField" : null,
					"textList" : [ "Hey, that's my line!" ],
					"inner" : null,
					"switch" : "broken",
					"approximation" : 123456.78,
					"moreAccurateApproximation" : null,
					"morePrecise" : null,
					"exceptionToUnwrappingRule" : { "opwvhk.resolvingTest.mustMatch" : {
						"number" : [ 1 ]
					} },
					"upgrade" : [ ],
					"category" : null
				}
				""");
	}

	@Test
	public void testResolvingAndParsingWithoutNamespace() throws IOException, SAXException {
		URL xsdLocation = requireNonNull(getClass().getResource("resolvingTest.xsd"));
		Schema readSchema = new Schema.Parser().parse(getClass().getResourceAsStream("resolvingTest.avsc"));
		XmlAsAvroParser parser = new XmlAsAvroParser(xsdLocation, "outer", readSchema, MODEL);

		GenericRecord resultMinimal = parser.parse(requireNonNull(getClass().getResource("resolvingTestMinimalWithoutNamespace.xml")));
		assertThat(toJson(resultMinimal)).isEqualToNormalizingWhitespace("""
				{
					"presentRequired" : "I'm here",
					"optionalField" : null,
					"textList" : [ "Hey, that's my line!" ],
					"inner" : null,
					"switch" : "broken",
					"approximation" : 123456.78,
					"moreAccurateApproximation" : null,
					"morePrecise" : null,
					"exceptionToUnwrappingRule" : { "opwvhk.resolvingTest.mustMatch" : {
						"number" : [ 1 ]
					} },
					"upgrade" : [ ],
					"category" : null
				}
				""");

		// If the data is missing required fields, parsing will succeed but yield an invalid object.

		GenericRecord resultTooMinimal = parser.parse(requireNonNull(getClass().getResource("resolvingTestTooMinimalWithoutNamespace.xml")));
		assertThat(resultTooMinimal).isNotNull();
		assertThatThrownBy(() -> toJson(resultTooMinimal)).isInstanceOf(NullPointerException.class); // Missing value for required field

		// If the data is outright invalid, parsing may fail (enums, for example, yield null value instead).

		URL invalidXmlLocation = requireNonNull(getClass().getResource("resolvingTestInvalidWithoutNamespace.xml"));
		assertThatThrownBy(() -> parser.parse(invalidXmlLocation)).isInstanceOf(Exception.class);
	}

	@Test
	public void testFailuresForFoo() throws IOException {
		URL xsdLocation = requireNonNull(getClass().getResource("resolvingTest.xsd"));
		Schema readSchema = new Schema.Parser().parse(getClass().getResourceAsStream("resolvingTest.avsc"));
		XmlAsAvroParser parser = new XmlAsAvroParser(xsdLocation, "outer", readSchema, MODEL);

		InputSource inputSource1 = new InputSource();
		inputSource1.setSystemId(requireNonNull(getClass().getResource("resolvingTestMinimalWithoutNamespace.xml")).toExternalForm());
		assertThatThrownBy(() -> parser.parse(inputSource1, true)).isInstanceOf(SAXException.class);

		InputSource inputSource3 = new InputSource();
		inputSource3.setSystemId(requireNonNull(getClass().getResource("resolvingTestInvalidWithNamespace.xml")).toExternalForm());
		assertThatThrownBy(() -> parser.parse(inputSource3, true)).isInstanceOf(SAXException.class);
	}

	@Test
	public void testContentOfMixedElements() throws IOException, SAXException {
		URL xsdLocation = requireNonNull(getClass().getResource("payload.xsd"));
		Schema readSchema = new Schema.Parser().parse(getClass().getResourceAsStream("envelope.avsc"));
		XmlAsAvroParser parser = new XmlAsAvroParser(xsdLocation, "envelope", readSchema, MODEL);

		GenericRecord resultBinary = parser.parse(requireNonNull(getClass().getResource("binaryPayload.xml")));
		assertThat(toJson(resultBinary)).isEqualToNormalizingWhitespace("""
				{
					"source" : "Bronsysteem",
					"target" : "Bestemming",
					"payload" : {
						"type" : { "opwvhk.resolvingTest.type" : "binary" },
						"value" : { "string" : "SGVsbG8gV29ybGQhCg==" }
					}
				}
				""");

		GenericRecord resultText = parser.parse(requireNonNull(getClass().getResource("textPayload.xml")));
		assertThat(toJson(resultText)).isEqualToNormalizingWhitespace("""
				{
					"source" : "Bronsysteem",
					"target" : "Bestemming",
					"payload" : {
						"type" : { "opwvhk.resolvingTest.type" : "text" },
						"value" : { "string" : "Hello World!" }
					}
				}
				""");

		String payload1 = """
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
				</record>""";
		GenericRecord resultXml = parser.parse(requireNonNull(getClass().getResource("xmlPayload.xml")));
		assertThat(toJson(resultXml)).isEqualToNormalizingWhitespace("""
				{
					"source" : "Bronsysteem",
					"target" : "Bestemming",
					"payload" : {
						"type" : { "opwvhk.resolvingTest.type" : "xml" },
						"value" : { "string" : "%s" }
					}
				}
				""".formatted(payload1.replace("\n", "\\n").replace("\t", "\\t").replace("\"", "\\\"")));

		String payload2 = """
				<record>

					<title>Status Report</title><summary language="NL">Een korte omschrijving</summary><status>OPEN</status><sequence/>
					<nested><description>No strings this time</description></nested>
				</record>""";
		GenericRecord resultDefault = parser.parse(requireNonNull(getClass().getResource("defaultAndCompactXmlPayload.xml")));
		assertThat(toJson(resultDefault)).isEqualToNormalizingWhitespace("""
				{
					"source" : "Bronsysteem",
					"target" : "Bestemming",
					"payload" : {
						"type" : { "opwvhk.resolvingTest.type" : "xml" },
						"value" : { "string" : "%s" }
					}
				}
				""".formatted(payload2.replace("\n", "\\n").replace("\t", "\\t").replace("\"", "\\\"")));
	}

	@Test
	public void testResolvingFailuresForScalars() {

		// Short though this list is, it covers all failure paths for scalar types.

		assertThatSchemasFailToResolve(FixedType.BOOLEAN, FixedType.STRING);

		assertThatSchemasFailToResolve(FixedType.STRING, FixedType.FLOAT);

		assertThatSchemasFailToResolve(enumType("A", List.of("A", "B"), null), enumType("A", List.of("B", "C"), "C"));

		assertThatSchemasFailToResolve(DecimalType.INTEGER_TYPE, FixedType.BOOLEAN);
		assertThatSchemasFailToResolve(DecimalType.INTEGER_TYPE, DecimalType.LONG_TYPE);
		assertThatSchemasFailToResolve(DecimalType.LONG_TYPE, DecimalType.integer(70, 22));
		assertThatSchemasFailToResolve(DecimalType.withFraction(6, 2), FixedType.FLOAT);
		assertThatSchemasFailToResolve(DecimalType.withFraction(6, 2), DecimalType.withFraction(5, 3));
		assertThatSchemasFailToResolve(DecimalType.withFraction(6, 2), DecimalType.withFraction(7, 1));

		assertThatSchemasFailToResolve(FixedType.BINARY_BASE64, FixedType.FLOAT);

		assertThatSchemasFailToResolve(FixedType.DOUBLE, FixedType.BINARY_HEX);

		assertThatSchemasFailToResolve(FixedType.STRING, struct("tooComplex").withFields(required("field", FixedType.STRING)));

		assertThatSchemasFailToResolve(FixedType.DATE, FixedType.BOOLEAN);
		assertThatSchemasFailToResolve(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)), FixedType.BOOLEAN);
		assertThatSchemasFailToResolve(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), FixedType.BOOLEAN);
		assertThatSchemasFailToResolve(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)), FixedType.BOOLEAN);
		assertThatSchemasFailToResolve(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), FixedType.BOOLEAN);
	}

	@Test
	public void testAllRequiredFieldsMustBeResolved() {
		assertThatSchemasFailToResolve(
				struct("read").withFields(
						required("name", FixedType.STRING),
						optional("description", FixedType.STRING)
				),
				struct("write").withFields(required("different", FixedType.STRING))
		);
		assertThatSchemasFailToResolve(
				struct("read").withFields(
						required("name", FixedType.STRING),
						optional("description", FixedType.STRING)
				),
				struct("write").withFields(required("name", FixedType.BINARY_BASE64))
		);
	}

	@Test
	public void testFieldsMustMatchCardinalityAndType() {
		// 2nd type is not a struct: there are no fields to match...
		assertThatSchemasFailToResolve(struct("tooComplex").withFields(required("field", FixedType.STRING)), FixedType.STRING);

		assertThatSchemasFailToResolve(
				struct("read").withFields(required("field", FixedType.STRING)),
				struct("write").withFields(optional("field", FixedType.STRING))
		);
		assertThatSchemasFailToResolve(
				struct("read").withFields(optional("field", FixedType.STRING)),
				struct("write").withFields(array("field", FixedType.STRING))
		);
		assertThatSchemasFailToResolve(
				struct("read").withFields(required("field", FixedType.STRING)),
				struct("write").withFields(required("field", FixedType.BINARY_BASE64))
		);

		assertThatSchemasFailToResolve(
				struct("read").withFields(required("field", FixedType.STRING)),
				struct("write").withFields(optional("field", FixedType.STRING))
		);
		assertThatSchemasFailToResolve(
				struct("read").withFields(required("field", FixedType.STRING)),
				struct("write").withFields(optional("field", FixedType.STRING))
		);

		assertThatSchemasFailToResolve(
				struct("read").withFields(array("field", FixedType.STRING)),
				struct("write").withFields(required("field", FixedType.BINARY_BASE64))
		);

		assertThatSchemasFailToResolve(Schema.createRecord("oops", null, null, false, List.of(
				new Schema.Field("nestedArray", Schema.createArray(Schema.createArray(Schema.create(Schema.Type.STRING))))
		)), struct("oops").withFields(array("nestedArray", FixedType.STRING)));

		assertThatSchemasFailToResolve(Schema.createRecord("oops", null, null, false, List.of(
				new Schema.Field("field", Schema.createArray(Schema.create(Schema.Type.STRING)))
		)), struct("oops").withFields(optional("field", struct("multipleFields").withFields(
				array("one", FixedType.STRING),
				array("two", FixedType.STRING)
		))));

		assertThatSchemasFailToResolve(Schema.createRecord("oops", null, null, false, List.of(
				new Schema.Field("field", Schema.createArray(
						Schema.createRecord("twoFields", null, null, false, List.of(
								new Schema.Field("one", Schema.create(Schema.Type.STRING)),
								new Schema.Field("two", Schema.create(Schema.Type.STRING))
						))
				))
		)), struct("oops").withFields(optional("field", struct("singleStringField").withFields(
				array("single", FixedType.STRING)
		))));
	}

	@Test
	public void testWrappedArrayFailures() {
		// Arrays can be "wrapped", but then:
		// * the wrapping struct (write type) cannot have multiple fields
		// * the read type should not be a wrapping type

		assertThatSchemasFailToResolve(
				struct("read").withFields(array("fields", FixedType.STRING)),
				struct("write").withFields(optional("fields", struct("wrapper").withFields(
						array("field", FixedType.STRING),
						optional("extra", FixedType.STRING)
				)))
		);

		assertThatSchemasFailToResolve(
				struct("read").withFields(array("fields", struct("singleField").withFields(
						required("cardinalityMismatch", FixedType.STRING)
				))),
				struct("write").withFields(optional("fields", struct("wrapper").withFields(
						array("cardinalityMismatch", FixedType.STRING)
				)))
		);
	}

	@Test
	public void coverMethodThatCannotBeCalled() {
		// There is no code path that actively causes this failure (that would mean a bug in building resolvers).
		assertThatThrownBy(() -> new ValueResolver() {}.addContent(null, null)).isInstanceOf(IllegalStateException.class);
	}

	private static void assertThatSchemasFailToResolve(Type readType, Type writeType) {
		assertThatSchemasFailToResolve(readType.toSchema(), writeType);
	}

	private static void assertThatSchemasFailToResolve(Schema readSchema, Type writeType) {
		assertThatThrownBy(() -> XmlAsAvroParser.resolve(writeType, readSchema, GenericData.get())).isInstanceOf(ResolvingFailure.class);
	}

	private static String toJson(GenericRecord record) throws IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		Schema schema = record.getSchema();
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, buffer, true);
		writer.write(record, encoder);
		encoder.flush();
		return buffer.toString(StandardCharsets.UTF_8); // JSON is UTF-8 encoded by spec
	}
}
