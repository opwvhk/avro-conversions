package opwvhk.avro;

import java.io.IOException;
import java.net.URL;

import org.apache.avro.Schema;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaManipulatorTest {
	@Test
	public void testSortedDocumentationViaXsd() throws IOException {
		URL xsdLocation = getClass().getResource("xml/payload.xsd");
		String markDownTable = SchemaManipulator
				.startFromXsd(xsdLocation, "envelope")
				.sortFields()
				.asMarkdownTable();

		assertThat(markDownTable).isEqualTo("""
				| Field(path) | Type | Documentation |
				|-------------|------|---------------|
				|  | record |  |
				| payload | record | The payload is either XML, UTF-8 text or base64 encoded binary data.<br/>Type: The payload is either XML, UTF-8 text or base64 encoded binary data. |
				| payload.type? | enum |  |
				| payload.value? | string | The entire element content, unparsed. |
				| source | string |  |
				| target | string |  |
				""");
	}

	@Test
	public void testDocumentationViaAvro() throws IOException {
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
	public void testManipulationsWithAliases() {
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
		assertThat(schema.toString(true)).isEqualTo(expectedSchema.toString(true));
	}

	@Test
	public void testManipulationsWithoutAliasesByPath() {
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
		assertThat(schema.toString(true)).isEqualTo(expectedSchema.toString(true));
	}

	@Test
	public void testUnwrappingArrays1() {
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_SCHEMA_WITH_ARRAYS)
				.unwrapArrayAtPath("matchByPath")
				.unwrapArrays(3)
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA_WITH_ARRAYS);
		assertThat(schema.toString(true)).isEqualTo(expectedSchema.toString(true));
	}

	@Test
	public void testUnwrappingArrays2() {
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_SCHEMA_WITH_ARRAYS)
				.unwrapArray("ns.WithArrays", "matchByName")
				.unwrapArrays(3)
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA_WITH_ARRAYS);
		assertThat(schema.toString(true)).isEqualTo(expectedSchema.toString(true));
	}

	@Test
	public void testManipulatingRecursiveSchemas() {
		// Note: manipulating by schema (and field name) also matches on aliases
		Schema schema = SchemaManipulator.startFromAvro(SOURCE_RECURSIVE_SCHEMA)
				.renameField("ns.recursive", "rabbitHole", "droste")
				.finish();

		Schema expectedSchema = new Schema.Parser().parse(EXPECTED_RECURSIVE_SCHEMA);
		assertThat(schema.toString(true)).isEqualTo(expectedSchema.toString(true));
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
