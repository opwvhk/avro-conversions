package opwvhk.avro.json;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import net.jimblackler.jsonschemafriend.GenerationException;
import opwvhk.avro.ResolvingFailure;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonAsAvroParserTest {
    @Test
    public void testMostTypesWithJsonSchema() throws IOException, GenerationException, URISyntaxException {
        Schema readSchema = avroSchema("TestRecord.avsc");

        JsonAsAvroParser parser = new JsonAsAvroParser(resourceUri("TestRecord.schema.json"), readSchema, GenericData.get());
        GenericRecord fullRecord = parser.parse(getClass().getResource("TestRecord-full.json"));

        assertThat(fullRecord.toString()).isEqualTo(
                ("{'bool': true, 'shortInt': 42, 'longInt': 6789012345, 'hugeInt': 123456789012345678901, 'defaultInt': 4242, " +
                 "'singleFloat': 123.456, 'doubleFloat': 1234.56789, 'fixedPoint': 12345678901.123456, 'defaultNumber': 98765.4321, " +
                 "'choice': 'maybe', 'date': '2023-04-17', 'time': '17:08:34.567123Z', 'timestamp': '2023-04-17T17:08:34.567123Z', " +
                 "'binary': 'Hello World!', 'hexBytes': 'Hello World!', 'texts': ['Hello', 'World!'], 'weirdStuff': {" +
                 "'explanation': 'No reason. I just felt like it.', 'fancy': '\uD83D\uDE04! You are on Candid Camera! \uD83D\uDCF9\uD83C\uDF4C', " +
                 "'rabbitHole': null}}").replace('\'', '"'));
    }

    @Test
    public void testParsingDatesAndTimesWithJsonSchema() throws IOException, URISyntaxException, GenerationException {
        Schema readSchema = avroSchema("DatesAndTimes.avsc");

        JsonAsAvroParser parser = new JsonAsAvroParser(resourceUri("DatesAndTimes.schema.json"), readSchema, GenericData.get());
        GenericRecord minimalRecord = parser.parse("""
                {"dateOnly": "2023-04-17", "timeMillis": "17:08:34.567+02:00", "timeMicros": "17:08:34.567123",
                 "timestampMillis": "2023-04-17T17:08:34.567123CET", "timestampMicros": "2023-04-17T17:08:34.567123+02:00",
                 "localTimestampMillis": "2023-04-17T17:08:34.567", "localTimestampMicros": "2023-04-17T17:08:34.567123"
                }""");

        assertThat(minimalRecord.toString()).isEqualTo(
                "{\"dateOnly\": \"2023-04-17\", \"timeMillis\": \"17:08:34.567+02:00\", \"timeMicros\": \"17:08:34.567123Z\", " +
                "\"timestampMillis\": \"2023-04-17T15:08:34.567123Z\", \"timestampMicros\": \"2023-04-17T15:08:34.567123Z\", " +
                "\"localTimestampMillis\": \"2023-04-17T17:08:34.567\", \"localTimestampMicros\": \"2023-04-17T17:08:34.567123\"}");
    }

    @Test
    public void testParsingEnumDifferentlyThanJsonSchema() throws IOException, URISyntaxException, GenerationException {
        URI jsonSchema = resourceUri("TestRecord.schema.json");
        JsonAsAvroParser parser;
        GenericRecord record;

        parser = new JsonAsAvroParser(jsonSchema, avroSchema("DifferentChoiceWithDefault.avsc"), GenericData.get());
        record = parser.parse("""
                {"choice": "no"}""");

        assertThat(record.toString()).isEqualTo(
                "{\"choice\": \"vielleicht\"}");

        parser = new JsonAsAvroParser(jsonSchema, avroSchema("ChoiceAsString.avsc"), GenericData.get());
        record = parser.parse("""
                {"choice": "no"}""");

        assertThat(record.toString()).isEqualTo(
                "{\"choice\": \"no\"}");
    }

    @Test
    public void testResolveFailuresWithJsonSchema() throws URISyntaxException {
        URI jsonSchema = resourceUri("TestRecord.schema.json");
        GenericData model = GenericData.get();

        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("RequiredShortInt.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("NotAnInt.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("TooShortDecimal.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("TooImpreciseDecimal.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("DifferentChoice.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("ChoiceAsInt.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("TooShortInteger.avsc"), model)).isInstanceOf(ResolvingFailure.class);
        assertThatThrownBy(() -> new JsonAsAvroParser(jsonSchema, avroSchema("NonNullableInt.avsc"), model)).isInstanceOf(ResolvingFailure.class);
    }

    @Test
    public void testMostTypesFromAvroSchema() throws IOException {
        Schema readSchema = avroSchema("TestRecordProjection.avsc");

        JsonAsAvroParser parser = new JsonAsAvroParser(readSchema, GenericData.get());
        GenericRecord fullRecord = parser.parse(getClass().getResource("TestRecord-full.json"));

        assertThat(fullRecord.toString()).isEqualTo(
                ("{'bool': true, 'shortInt': 42, 'longInt': 6789012345, 'hugeInt': 123456789012345678901, 'defaultInt': 4242, " +
                 "'singleFloat': 123.456, 'doubleFloat': 1234.56789, 'fixedPoint': 12345678901.123456, 'defaultNumber': 98765.4321, " +
                 "'choice': 'maybe', 'date': '2023-04-17', 'time': '17:08:34.567123Z', 'timestamp': '2023-04-17T17:08:34.567123Z', " +
                 "'texts': ['Hello', 'World!'], 'weirdStuff': {'explanation': 'No reason. I just felt like it.', " +
                 "'fancy': '\uD83D\uDE04! You are on Candid Camera! \uD83D\uDCF9\uD83C\uDF4C', 'rabbitHole': null}}").replace('\'', '"'));
    }

    @Test
    public void testMinimalRecordFromAvroSchema() throws IOException {
        Schema readSchema = avroSchema("TestRecordProjection.avsc");

        JsonAsAvroParser parser = new JsonAsAvroParser(readSchema, GenericData.get());
        GenericRecord minimalRecord = parser.parse("""
                {
                    "bool": false,
                    "shortInt": null,
                    "choice": "no",
                    "texts": [],
                    "weirdStuff": { }
                }""");

        assertThat(minimalRecord.toString()).isEqualTo(
                ("{'bool': false, 'shortInt': null, 'longInt': null, 'hugeInt': null, 'defaultInt': 42, " +
                 "'singleFloat': null, 'doubleFloat': null, 'fixedPoint': null, 'defaultNumber': 4.2, 'choice': 'no', " +
                 "'date': null, 'time': null, 'timestamp': null, " +
                 "'texts': [], 'weirdStuff': {'explanation': 'Please explain why', " +
                 "'fancy': null, 'rabbitHole': null}}").replace('\'', '"'));
    }

    @Test
    public void testParsingDatesAndTimesFromAvro() throws IOException {
        Schema readSchema = avroSchema("DatesAndTimes.avsc");

        JsonAsAvroParser parser = new JsonAsAvroParser(readSchema, GenericData.get());
        GenericRecord minimalRecord = parser.parse("""
                {"dateOnly": "2023-04-17", "timeMillis": "17:08:34.567+02:00", "timeMicros": "17:08:34.567123",
                 "timestampMillis": "2023-04-17T17:08:34.567123CET", "timestampMicros": "2023-04-17T17:08:34.567123+02:00",
                 "localTimestampMillis": "2023-04-17T17:08:34.567", "localTimestampMicros": "2023-04-17T17:08:34.567123"
                }""");

        assertThat(minimalRecord.toString()).isEqualTo(
                "{\"dateOnly\": \"2023-04-17\", \"timeMillis\": \"17:08:34.567+02:00\", \"timeMicros\": \"17:08:34.567123Z\", " +
                "\"timestampMillis\": \"2023-04-17T15:08:34.567123Z\", \"timestampMicros\": \"2023-04-17T15:08:34.567123Z\", " +
                "\"localTimestampMillis\": \"2023-04-17T17:08:34.567\", \"localTimestampMicros\": \"2023-04-17T17:08:34.567123\"}");
    }

    @Test
    public void testParsingObjectsAndArraysForScalarsFails() {
        Schema readSchema = new Schema.Parser().parse("""
                {"type": "record", "name": "Oops", "fields": [
                  {"name": "text", "type": "string"}
                ]}""");

        JsonAsAvroParser parser = new JsonAsAvroParser(readSchema, GenericData.get());
        assertThatThrownBy(() -> parser.parse("{\"text\": {}}")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> parser.parse("{\"text\": []}")).isInstanceOf(IllegalStateException.class);
    }

    private Schema avroSchema(String avroSchemaResource) throws IOException {
        try (InputStream expectedSchemaStream = getClass().getResourceAsStream(avroSchemaResource)) {
            return new Schema.Parser().parse(expectedSchemaStream);
        }
    }

    private URI resourceUri(String resource) throws URISyntaxException {
        return Objects.requireNonNull(getClass().getResource(resource)).toURI();
    }
}
