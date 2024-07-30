package opwvhk.avro.json;

import net.jimblackler.jsonschemafriend.GenerationException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaAnalyzerTest {
	@Test
	void testSchemaVersionDraft4() throws URISyntaxException {
		SchemaProperties schemaProperties = parseSchemaResource("draft4-schema.json");
		// * range of "number" is not an integer range: type "integer" is NOT inferred because bounds have a fraction
		// * number range has boolean properties for exclusive bounds
		// * 'contentEncoding', 'if', 'then', 'else' are ignored
		// * enums do not accept the 'const' keyword
		// * arrays are defined using 'items' (a schema or an array) & 'additionalItems' (if 'items' is an array), 'contains' is ignored
		assertThat(schemaString(schemaProperties)).isEqualTo(
				"SchemaProperties{ title='TestCase', types=[OBJECT], numberRange=(-inf, inf), requiredProperties=[number, choice, text], properties={" +
				"number=SchemaProperties{" +
				" title='number', types=[NUMBER], numberRange=[0, 123.0)}," +
				" choice=SchemaProperties{ title='choice', types=[STRING], numberRange=(-inf, inf), enumValues=[no, maybe]}," +
				" text=SchemaProperties{ title='text', types=[STRING, NULL], numberRange=(-inf, inf), defaultValue='abc'}," +
				" names=SchemaProperties{ title='names', types=[ARRAY], numberRange=(-inf, inf), itemSchemaProperties=SchemaProperties{" +
				" title='additionalItems', types=[STRING, NUMBER, NULL], numberRange=(-inf, inf)}}" +
				"}}");
	}

	@Test
	void testSchemaVersionDraft6() throws URISyntaxException {
		SchemaProperties schemaProperties = parseSchemaResource("draft6-schema.json");
		// * range of "number" is an integer range: type "integer" is inferred as no bounds have a non-zero fraction
		// * number range has separate number properties for exclusive bounds
		// * 'contentEncoding', 'if', 'then', 'else' are ignored
		// * enums now also accept the 'const' keyword
		// * arrays are defined using 'items' (a schema or an array) & 'additionalItems' (if 'items' is an array), 'contains' is applied
		assertThat(schemaString(schemaProperties)).isEqualTo(
				"SchemaProperties{ title='TestCase', types=[OBJECT], numberRange=(-inf, inf), requiredProperties=[number, choice, text], properties={" +
				"number=SchemaProperties{ title='number', types=[INTEGER, NUMBER], numberRange=[0, 123.0)}, " +
				"choice=SchemaProperties{ title='choice', types=[STRING], numberRange=(-inf, inf), enumValues=[yes]}, " +
				"text=SchemaProperties{ title='text', types=[STRING, NULL], numberRange=(-inf, inf), defaultValue='abc'}, " +
				"names=SchemaProperties{ title='names', types=[ARRAY], numberRange=(-inf, inf), itemSchemaProperties=SchemaProperties{" +
				" title='items', types=[BOOLEAN, NULL], numberRange=(-inf, inf)}}" +
				"}}");
	}

	@Test
	void testSchemaVersionDraft7() throws URISyntaxException {
		SchemaProperties schemaProperties = parseSchemaResource("draft7-schema.json");
		// * 'contentEncoding', 'if', 'then', 'else' are now applied
		assertThat(schemaString(schemaProperties)).isEqualTo(
				"SchemaProperties{ title='TestCase', types=[OBJECT], numberRange=(-inf, inf), requiredProperties=[number, choice], properties={" +
				"choice=SchemaProperties{ title='choice', types=[STRING], numberRange=(-inf, inf), enumValues=[maybe]}, " +
				"bytes=SchemaProperties{ title='bytes', types=[STRING], numberRange=(-inf, inf), contentEncoding='base64'}, " +
				"missing=SchemaProperties{ title='missing', types=[STRING], numberRange=(-inf, inf)}, " +
				"reason=SchemaProperties{ title='reason', types=[STRING], numberRange=(-inf, inf)}" +
				"}}");
	}

	@Test
	void testUnknownSchemaVersion() throws URISyntaxException {
		SchemaProperties schemaProperties = parseSchemaResource("unspecified-schema.json");
		// Unknown schema causes no references to be resolved by the underlying library
		assertThat(schemaString(schemaProperties)).isEqualTo(
				"SchemaProperties{ title='TestCase', description='Test schema for the JSON schema analyser.', " +
				"types=[OBJECT, ARRAY, STRING, INTEGER, NUMBER, BOOLEAN, NULL], numberRange=(-inf, inf)}");
	}

	@Test
	void testSchemaVersionDraft2020() throws URISyntaxException {
		SchemaProperties schemaProperties = parseSchemaResource("draft2020-12-schema.json");
		// * arrays are defined using 'prefixItems' (an array) & 'items'; 'contains' remains valid, also use 'unevaluatedItems'
		// * additional structures to test all code paths missed so far
		assertThat(schemaString(schemaProperties)).isEqualTo(
				"SchemaProperties{" +
				" title='TestCase', description='Test schema for the JSON schema analyser.', types=[OBJECT], numberRange=(-inf, inf), properties={" +
				"mixedBag=SchemaProperties{ title='mixedBag', types=[ARRAY], numberRange=(-inf, inf)," +
				" itemSchemaProperties=SchemaProperties{ title='items', types=[STRING, NUMBER, BOOLEAN, NULL], numberRange=(-inf, inf)}}, " +
				"list=SchemaProperties{ title='list', types=[ARRAY], numberRange=(-inf, inf)," +
				" itemSchemaProperties=SchemaProperties{ title='items', types=[STRING], numberRange=(-inf, inf)}}, " +
				"smallNumber=SchemaProperties{ title='int', types=[INTEGER, NUMBER], numberRange=(0, 2147483648)}, " +
				"multipleChoice=SchemaProperties{ title='multipleChoice', types=[STRING], numberRange=(-inf, inf), enumValues=[x, y, [z]]}, " +
				"stillAnEnum=SchemaProperties{ title='stillAnEnum', types=[STRING], numberRange=(-inf, inf), enumValues=[x, y]}, " +
				"droste=SchemaProperties{ title='Droste', types=[OBJECT], numberRange=(-inf, inf), properties={coffeeFlavour=SchemaProperties{" +
				" title='coffeeFlavour', types=[STRING], numberRange=(-inf, inf)}, droste=SchemaProperties}}" +
				"}}");
	}

	private SchemaProperties parseSchemaResource(String schemaResource) throws URISyntaxException {
		URI jsonSchemaLocation = requireNonNull(getClass().getResource(schemaResource)).toURI();
		return new SchemaAnalyzer().parseJsonProperties(jsonSchemaLocation);
	}

	private Schema parseSchemaResourceAsAvro(String schemaResource) throws GenerationException, URISyntaxException {
		URI jsonSchemaLocation = requireNonNull(getClass().getResource(schemaResource)).toURI();
		return new SchemaAnalyzer().parseJsonSchema(jsonSchemaLocation);
	}

	private String schemaString(SchemaProperties schemaProperties) {
		return schemaProperties.toString().replaceAll("@[a-fA-F\\d]+", "");
	}

	@Test
	void verifyTypesFromSchemaProperties() {
		SchemaProperties schemaProperties = new SchemaProperties(false);
		assertThat(schemaProperties)
				.hasFieldOrPropertyWithValue("nullable", false)
				.hasFieldOrPropertyWithValue("type", null);

		schemaProperties.addType(SchemaType.NULL);
		assertThat(schemaProperties)
				.hasFieldOrPropertyWithValue("nullable", true)
				.hasFieldOrPropertyWithValue("type", null);

		schemaProperties.addType(SchemaType.STRING);
		schemaProperties.addType(SchemaType.INTEGER);
		assertThat(schemaProperties)
				.hasFieldOrPropertyWithValue("nullable", true)
				.hasFieldOrPropertyWithValue("type", SchemaType.STRING);

		schemaProperties.setTypes(EnumSet.of(SchemaType.STRING, SchemaType.ARRAY, SchemaType.BOOLEAN));
		assertThat(schemaProperties)
				.hasFieldOrPropertyWithValue("nullable", false)
				.hasFieldOrPropertyWithValue("type", SchemaType.ARRAY);
	}

	@Test
	void testAvroSchemaConversion() throws URISyntaxException, GenerationException, IOException {
		assertThatThrownBy(() -> parseSchemaResourceAsAvro("invalid.schema.json")).isInstanceOf(AnalysisFailure.class);
		assertThatThrownBy(() -> parseSchemaResourceAsAvro("null.schema.json")).isInstanceOf(IllegalArgumentException.class);

		Schema avroSchema = parseSchemaResourceAsAvro("TestRecord.schema.json");

		Schema expectedSchema;
		try (InputStream expectedSchemaStream = getClass().getResourceAsStream("TestRecordAll.avsc")) {
			expectedSchema = new Schema.Parser().parse(expectedSchemaStream);
		}
		assertThat(avroSchema.toString(true)).isEqualTo(expectedSchema.toString(true));
	}
}
