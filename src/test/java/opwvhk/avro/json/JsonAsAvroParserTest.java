package opwvhk.avro.json;

import java.io.IOException;
import java.io.InputStream;

import opwvhk.avro.io.AsAvroParserBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonAsAvroParserTest {
    @Test
    public void testMostTypesFromAvroSchema() throws IOException {
        Schema readSchema;
        try (InputStream expectedSchemaStream = getClass().getResourceAsStream("TestRecordProjection.avsc")) {
            readSchema = new Schema.Parser().parse(expectedSchemaStream);
        }

        JsonAsAvroParser parser = new JsonAsAvroParser(readSchema, GenericData.get());
        GenericRecord fullRecord = parser.parse(getClass().getResource("TestRecord-full.json"));

        assertThat(fullRecord.toString()).isEqualTo(
                ("{'bool': true, 'shortInt': 42, 'longInt': 6789012345, 'hugeInt': 123456789012345678901, 'defaultInt': 4242, " +
                 "'singleFloat': 123.456, 'doubleFloat': 1234.56789, 'fixedPoint': 12345678901.123456, 'choice': 'maybe', " +
                 "'date': '2023-04-17', 'time': '17:08:34.567123Z', 'timestamp': '2023-04-17T17:08:34.567123Z', " +
                 "'texts': ['Hello', 'World!'], 'weirdStuff': {'explanation': 'No reason. I just felt like it.', " +
                 "'fancy': '\uD83D\uDE04! You are on Candid Camera! \uD83D\uDCF9\uD83C\uDF4C'}}").replace('\'', '"'));
    }

    @Test
    public void testMinimalRecordFromAvroSchema() throws IOException {
        Schema readSchema;
        try (InputStream expectedSchemaStream = getClass().getResourceAsStream("TestRecordProjection.avsc")) {
            readSchema = new Schema.Parser().parse(expectedSchemaStream);
        }

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
                 "'singleFloat': null, 'doubleFloat': null, 'fixedPoint': null, 'choice': 'no', " +
                 "'date': null, 'time': null, 'timestamp': null, " +
                 "'texts': [], 'weirdStuff': {'explanation': 'Please explain why', " +
                 "'fancy': null}}").replace('\'', '"'));
    }

    @Test
    public void testParsingDatesAndTimes() throws IOException {
        Schema readSchema = new Schema.Parser().parse("""
                {"type": "record", "name": "DatesAndTimes", "fields": [
                  {"name": "dateOnly", "type": {"type": "int", "logicalType": "date"}},
                  {"name": "timeMillis", "type": {"type": "int", "logicalType": "time-millis"}},
                  {"name": "timeMicros", "type": {"type": "long", "logicalType": "time-micros"}},
                  {"name": "timestampMillis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                  {"name": "timestampMicros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                  {"name": "localTimestampMillis", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
                  {"name": "localTimestampMicros", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
                ]}""");

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
                {"type": "record", "name": "Woopsie", "fields": [
                  {"name": "text", "type": "string"}
                ]}""");

        JsonAsAvroParser parser = new JsonAsAvroParser(readSchema, GenericData.get());
        assertThatThrownBy(() -> parser.parse("{\"text\": {}}")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> parser.parse("{\"text\": []}")).isInstanceOf(IllegalStateException.class);
    }
}
