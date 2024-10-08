package opwvhk.avro;

import opwvhk.avro.util.NamingConvention;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaFormatter;
import org.apache.avro.SchemaParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class SchemaManipulatorTest {
	private static final SchemaFormatter FORMATTER = SchemaFormatter.getInstance("json");
	@Test
	void testSortedDocumentationViaXsd() throws IOException {
		StringBuilder markdown = new StringBuilder();

		URL xsdLocation = getClass().getResource("xml/payload.xsd");
		Schema envelopeSchema = SchemaManipulator
				.startFromXsd(xsdLocation, "envelope")
				.sortFields()
				.alsoDocumentAsMarkdownTable(markdown)
				.finish();

		assertThat(markdown).isEqualToNormalizingUnicode("""
				| Field(path) | Type | Documentation |
				|-------------|------|---------------|
				|  | record |  |
				| payload | record | The payload is either XML, UTF-8 text or base64 encoded binary data.<br/>Type: The payload is either XML, UTF-8 text or base64 encoded binary data. |
				| payload.type? | enum |  |
				| payload.value? | string | The entire element content, unparsed. |
				| source | string |  |
				| target | string |  |
				""");
		assertThat(FORMATTER.format(envelopeSchema)).isEqualTo(FORMATTER.format(Schema.createRecord("ns.envelope", null, null, false, List.of(
				new Schema.Field("payload",
						Schema.createRecord("ns.payload", "The payload is either XML, UTF-8 text or base64 encoded binary data.", null, false, List.of(
								new Schema.Field("type", Schema.createUnion(Schema.createEnum("ns.type", null, null, List.of("xml", "text", "binary")),
										Schema.create(Schema.Type.NULL)), null, "xml"),
								new Schema.Field("value", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)),
										"The entire element content, unparsed.", Schema.Field.NULL_DEFAULT_VALUE)
						)), "The payload is either XML, UTF-8 text or base64 encoded binary data.", null),
				new Schema.Field("source", Schema.create(Schema.Type.STRING)),
				new Schema.Field("target", Schema.create(Schema.Type.STRING))
		))));
	}

	@Test
	void testSortedDocumentationViaJsonSchema() throws URISyntaxException, IOException {
		StringBuilder markdown = new StringBuilder();

		URL schemaLocation = getClass().getResource("json/TestRecord.schema.json");
		Schema schema = SchemaManipulator
				.startFromJsonSchema(requireNonNull(schemaLocation))
				.alsoDocumentAsMarkdownTable(markdown)
				.finish();

		assertThat(markdown).isEqualToNormalizingUnicode("""
				| Field(path) | Type | Documentation |
				|-------------|------|---------------|
				|  | record | Type: Test schema for parsing records. |
				| bool | boolean |  |
				| shortInt? | int |  |
				| longInt? | long |  |
				| hugeInt? | decimal(21,0) |  |
				| defaultInt? | long |  |
				| singleFloat? | float |  |
				| doubleFloat? | double |  |
				| fixedPoint? | decimal(17,6) |  |
				| defaultNumber? | double |  |
				| choice | enum |  |
				| date? | date |  |
				| time? | time-millis |  |
				| timestamp? | timestamp-millis |  |
				| binary? | bytes |  |
				| hexBytes? | bytes |  |
				| texts[] | string |  |
				| weirdStuff? | record |  |
				| weirdStuff?.explanation? | string |  |
				| weirdStuff?.fancy? | string |  |
				| weirdStuff?.hatseflats? | string |  |
				| weirdStuff?.rabbitHole? | record |  |
				""");
		assertThat(SchemaFormatter.format("json", schema)).isEqualTo(readAndFormatSchema("json/TestRecordAll.avsc"));
	}

	private String readAndFormatSchema(@SuppressWarnings("SameParameterValue") String resource) throws IOException {
		SchemaParser parser = new SchemaParser();
		SchemaParser.ParseResult parseResult = parser.parse(getClass().getResourceAsStream(resource));
		Schema schema = parseResult.mainSchema();
		return SchemaFormatter.format("json", schema);
	}

	@Test
	void testDocumentationViaAvro() throws IOException {
		URL avroLocation = getClass().getResource("xml/envelope.avsc");
		String markDownTable = SchemaManipulator.startFromAvro(avroLocation).asMarkdownTable();

		assertThat(markDownTable).isEqualTo("""
				| Field(path) | Type | Documentation |
				|-------------|------|---------------|
				|  | record |  |
				| source | string |  |
				| target | string |  |
				| payload | record | The payload is either XML, UTF-8 text or base64 encoded binary data.<br/>Type: The payload is either XML, UTF-8 text or base64 encoded binary data. |
				| payload.type? | enum |  |
				| payload.value? | string | The entire element content, unparsed. |
				""");
	}

	@Test
	void testManipulationsWithAliases() {
		// Note: manipulating by schema (and field name) also matches on aliases
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_SCHEMA)
				.renameSchema("ns.envelope", "ns.satchel")
				.renameSchema("ns.switch", "ns.toggle")
				.renameSchema("ns.hash", "ns.salted")
				.renameField("ns.envelope", "target", "destination")
				.renameField("ns.envelope", "properties", "extraProperties")
				.renameField("ns.payloadRecord", "digestsByName", "namedHashes")
				.unwrapArray("ns.envelope", "wrappingField")
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA);
		assertThat(FORMATTER.format(schema)).isEqualTo(FORMATTER.format(expectedSchema));
	}

	@Test
	void testManipulationsWithoutAliasesByPath() {
		// Note: manipulating by path cannot match on aliases
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_SCHEMA)
				.renameWithoutAliases()
				.renameSchemaAtPath("ns.satchel")
				.renameSchemaAtPath("ns.salted", "payload", "digestsByName")
				.renameSchemaAtPath("ns.toggle", "payload", "switchList")
				.renameFieldAtPath("destination", "target")
				.renameFieldAtPath("namedHashes", "payload", "digestsByName")
				.unwrapArrayAtPath("nested")
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA_WITHOUT_ALIASES);
		assertThat(FORMATTER.format(schema)).isEqualTo(FORMATTER.format(expectedSchema));
	}

	@Test
	void testUnwrappingArrays1() {
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_SCHEMA_WITH_ARRAYS)
				.unwrapArrayAtPath("matchByPath")
				.unwrapArrays(3)
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA_WITH_ARRAYS);
		assertThat(FORMATTER.format(schema)).isEqualTo(FORMATTER.format(expectedSchema));
	}

	@Test
	void testUnwrappingArrays2() {
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_SCHEMA_WITH_ARRAYS)
				.unwrapArray("ns.WithArrays", "matchByName")
				.unwrapArrays(3)
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA_WITH_ARRAYS);
		assertThat(FORMATTER.format(schema)).isEqualTo(FORMATTER.format(expectedSchema));
	}

	@Test
	void testManipulatingRecursiveSchemas() {
		// Note: manipulating by schema (and field name) also matches on aliases
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_RECURSIVE_SCHEMA)
				.renameField("ns.recursive", "rabbitHole", "droste")
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_RECURSIVE_SCHEMA);
		assertThat(FORMATTER.format(schema)).isEqualTo(FORMATTER.format(expectedSchema));
	}

	@Test
	void testApplyNamingConventions() {
		Schema schemaWithoutNamespace = SchemaBuilder.record("simple_name").fields()
				.name("field_1").type("string").noDefault()
				.name("field2").type("string").noDefault()
				.endRecord();
		Schema schemaResultWithoutNamespace = SchemaBuilder.record("simple_name").fields()
				.name("field1").type("string").noDefault()
				.name("field2").type("string").noDefault()
				.endRecord();
		assertThat(FORMATTER.format(new SchemaManipulator(schemaWithoutNamespace).renameWithoutAliases()
				.useSchemaNamingConvention(NamingConvention.SNAKE_CASE)
				.useFieldNamingConvention(NamingConvention.CAMEL_CASE)
				.finish())
		).isEqualTo(FORMATTER.format(schemaResultWithoutNamespace));

		Schema schemaWithNamespace = SchemaBuilder.record("simple_name").namespace("somewhereInTheCode").fields()
				.name("field_one").type("string").noDefault()
				.name("field_two").type("string").noDefault()
				.endRecord();
		Schema schemaResultWithNamespace = SchemaBuilder.record("SimpleName").namespace("somewhere_in_the_code").fields()
				.name("fieldOne").type("string").noDefault()
				.name("fieldTwo").type("string").noDefault()
				.endRecord();
		assertThat(FORMATTER.format(new SchemaManipulator(schemaWithNamespace).renameWithoutAliases()
				.useSchemaNamingConvention(NamingConvention.SNAKE_CASE, NamingConvention.PASCAL_CASE)
				.useFieldNamingConvention(NamingConvention.CAMEL_CASE)
				.finish())
		).isEqualTo(FORMATTER.format(schemaResultWithNamespace));
	}

	private static final String SOURCE_SCHEMA = """
			{
				"testCase": "SchemaManipulatorTest",
				"type": "record",
				"name": "envelope",
				"namespace": "ns",
				"fields": [
					{
						"extra": "unused",
						"name": "source",
						"type": "string"
					}, {
						"name": "target",
						"type": "string"
					}, {
						"name": "payload",
						"type": {
							"type": "record",
							"name": "payloadRecord",
							"doc": "The payload is either XML, UTF-8 text or base64 encoded binary data.",
							"fields": [
								{
									"name": "type",
									"type": [
										{
											"type": "enum",
											"name": "type",
											"symbols": ["xml", "text", "binary"]
										}, "null"
									],
									"default": "xml"
								}, {
									"name": "value",
									"type": ["null", "string"],
									"doc": "The entire element content, unparsed.",
									"default": null
								}, {
									"name": "digestsByName",
									"type": {"type": "map", "values": {
										"type": "fixed",
										"name": "hash",
										"size": 16
									}},
									"default": {}
								}, {
									"name": "hmac",
									"type": {
										"type": "fixed",
										"name": "hmac",
										"size": 32
									}
								}, {
									"name": "switchList",
									"type": {"type": "array", "items": {
										"type": "enum",
										"name": "switch",
										"symbols": ["off", "on"]
									}},
									"default": []
								}, {
									"name": "category",
									"type": {
										"type": "enum",
										"name": "types",
										"symbols": ["good", "bad", "ugly"],
										"default": "ugly"
									},
									"default": "good"
								}
							]
						},
						"doc": "The payload is either XML, UTF-8 text or base64 encoded binary data."
					}, {
						"name": "nested",
						"aliases": ["wrappingField"],
						"type": {
							"type": "record",
							"name": "WithSingleField",
							"fields": [{
								"name": "ignored",
								"type": {
									"type": "array",
									"items": "string"
								},
								"default": []
							}]
						}
					}
				]
			}""";
	private static final String EXPECTED_SCHEMA = """
			{
				"testCase": "SchemaManipulatorTest",
				"type": "record",
				"name": "satchel",
				"namespace": "ns",
				"aliases": ["ns.envelope"],
				"fields": [
					{
						"extra": "unused",
						"name": "source",
						"type": "string"
					}, {
						"name": "destination",
						"aliases": ["target"],
						"type": "string"
					}, {
						"name": "payload",
						"type": {
							"type": "record",
							"name": "payloadRecord",
							"doc": "The payload is either XML, UTF-8 text or base64 encoded binary data.",
							"fields": [
								{
									"name": "type",
									"type": [
										{
											"type": "enum",
											"name": "type",
											"symbols": ["xml", "text", "binary"]
										}, "null"
									],
									"default": "xml"
								}, {
									"name": "value",
									"type": ["null", "string"],
									"doc": "The entire element content, unparsed.",
									"default": null
								}, {
									"name": "namedHashes",
									"aliases": ["digestsByName"],
									"type": {"type": "map", "values": {
										"type": "fixed",
										"name": "salted",
										"size": 16
									}},
									"default": {}
								}, {
									"name": "hmac",
									"type": {
										"type": "fixed",
										"name": "hmac",
										"size": 32
									}
								}, {
									"name": "switchList",
									"type": {"type": "array", "items": {
										"type": "enum",
										"name": "toggle",
										"symbols": ["off", "on"]
									}},
									"default": []
								}, {
									"name": "category",
									"type": {
										"type": "enum",
										"name": "types",
										"symbols": ["good", "bad", "ugly"],
										"default": "ugly"
									},
									"default": "good"
								}
							]
						},
						"doc": "The payload is either XML, UTF-8 text or base64 encoded binary data."
					}, {
						"name": "nested",
						"aliases": ["wrappingField"],
						"type": {
							"type": "array",
							"items": "string"
						},
						"default": []
					}
				]
			}""";
	private static final String EXPECTED_SCHEMA_WITHOUT_ALIASES = """
			{
				"testCase": "SchemaManipulatorTest",
				"type": "record",
				"name": "satchel",
				"namespace": "ns",
				"fields": [
					{
						"extra": "unused",
						"name": "source",
						"type": "string"
					}, {
						"name": "destination",
						"type": "string"
					}, {
						"name": "payload",
						"type": {
							"type": "record",
							"name": "payloadRecord",
							"doc": "The payload is either XML, UTF-8 text or base64 encoded binary data.",
							"fields": [
								{
									"name": "type",
									"type": [
										{
											"type": "enum",
											"name": "type",
											"symbols": ["xml", "text", "binary"]
										}, "null"
									],
									"default": "xml"
								}, {
									"name": "value",
									"type": ["null", "string"],
									"doc": "The entire element content, unparsed.",
									"default": null
								}, {
									"name": "namedHashes",
									"type": {"type": "map", "values": {
										"type": "fixed",
										"name": "salted",
										"size": 16
									}},
									"default": {}
								}, {
									"name": "hmac",
									"type": {
										"type": "fixed",
										"name": "hmac",
										"size": 32
									}
								}, {
									"name": "switchList",
									"type": {"type": "array", "items": {
										"type": "enum",
										"name": "toggle",
										"symbols": ["off", "on"]
									}},
									"default": []
								}, {
									"name": "category",
									"type": {
										"type": "enum",
										"name": "types",
										"symbols": ["good", "bad", "ugly"],
										"default": "ugly"
									},
									"default": "good"
								}
							]
						},
						"doc": "The payload is either XML, UTF-8 text or base64 encoded binary data."
					}, {
						"name": "nested",
						"aliases": ["wrappingField"],
						"type": {
							"type": "array",
							"items": "string"
						},
						"default": []
					}
				]
			}""";

	private static final String SOURCE_SCHEMA_WITH_ARRAYS = """
			{
				"type": "record",
				"namespace": "ns",
				"name": "WithArrays",
				"fields": [
					{"name": "notARecord", "type": ["int", "string"], "doc": "this and the nest field also increase coverage on union checks"},
					{"name": "alsoNotARecord", "type": ["int", "string", "null"]},
					{"name": "tooLargeRecord", "type": { "type": "record", "name": "pair", "fields": [
						{"name": "one", "type": "int"}, {"name": "two", "type": "int"}
					]}},
					{"name": "notAWrappedArray", "type": {"type": "record", "name": "nested", "fields": [
						{"name": "numbers", "type": {"type": "record", "name": "wrappedNumbers",
						 "doc": "This wrapped array is nested in a single-field record to increase test coverage twice: numbers is not an array, and to not match on wrapping schema name",
						 "fields": [
							{"name": "numberArray", "type": {"type": "array", "items": "int"}, "default": []}
						]}}
					]}},
					{"name": "dependencies", "type": ["null", {"type": "record", "name": "whatever", "fields": [
						{"name": "dependency", "type": {"type": "array", "items": "string"}, "default": []}
					]}], "default": null},
					{"name": "matchByPath", "aliases": ["matchByName"], "type": {"type": "record", "name": "wrappedStrings", "fields": [
						{"name": "textList", "type": {"type": "array", "items": "string"}}
					]}},
					{"name": "anotherOne", "type": {"type": "record", "name": "wrappedEnums", "fields": [
						{"name": "keptIntact", "type": {"type": "array", "items": {"type": "enum", "name": "switch", "symbols": ["on", "off"]}}}
					]}}
				]
			}""";
	private static final String EXPECTED_SCHEMA_WITH_ARRAYS = """
			{
				"type": "record",
				"namespace": "ns",
				"name": "WithArrays",
				"fields": [
					{"name": "notARecord", "type": ["int", "string"], "doc": "this and the nest field also increase coverage on union checks"},
					{"name": "alsoNotARecord", "type": ["int", "string", "null"]},
					{"name": "tooLargeRecord", "type": { "type": "record", "name": "pair", "fields": [
						{"name": "one", "type": "int"}, {"name": "two", "type": "int"}
					]}},
					{"name": "notAWrappedArray", "type": {"type": "record", "name": "nested", "fields": [
						{"name": "numbers", "type": {"type": "array", "items": "int"}, "default": []}
					]}},
					{"name": "dependencies", "type": {"type": "array", "items": "string"}, "default": []},
					{"name": "matchByPath", "aliases": ["matchByName"], "type": {"type": "array", "items": "string"}},
					{"name": "anotherOne", "type": {"type": "record", "name": "wrappedEnums", "fields": [
						{"name": "keptIntact", "type": {"type": "array", "items": {"type": "enum", "name": "switch", "symbols": ["on", "off"]}}}
					]}}
				]
			}""";

	private static final String SOURCE_RECURSIVE_SCHEMA = """
			{
				"testCase": "SchemaManipulatorTest",
				"type": "record",
				"name": "recursive",
				"namespace": "ns",
				"fields": [
					{
						"name": "name",
						"type": "string"
					}, {
						"name": "rabbitHole",
						"type": "recursive"
					}
				]
			}""";
	private static final String EXPECTED_RECURSIVE_SCHEMA = """
			{
				"testCase": "SchemaManipulatorTest",
				"type": "record",
				"name": "recursive",
				"namespace": "ns",
				"fields": [
					{
						"name": "name",
						"type": "string"
					}, {
						"name": "droste",
						"aliases": ["rabbitHole"],
						"type": "recursive"
					}
				]
			}""";
}
