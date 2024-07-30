package opwvhk.avro.io;

import opwvhk.avro.ResolvingFailure;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParser;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsAvroParserBaseTest {
    /*
    NOTE: This test class is terribly incomplete, as the resolvers it creates cannot be tested without parsing (which this test doesn't cover).
    */

	@Test
	void testTimeZoneOffsetDetermination() {
		Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(1681720200000L), UTC); // 2023-04-17T08:30:00.000000000Z

		assertThat(AsAvroParserBase.asOffset(UTC, fixedClock)).isEqualTo(ZoneOffset.ofHours(0));

		assertThat(AsAvroParserBase.asOffset(ZoneId.of("Europe/Amsterdam"), fixedClock)).isEqualTo(ZoneOffset.ofHours(2));
	}

	@Test
	void testBinaryResolverFailures() {
		AsAvroParserBase<Object> parserBase = new AsAvroParserBase<>(GenericData.get(), null, Schema.create(Schema.Type.STRING), Set.of(),
				ZoneOffset.ofHours(0)) {
		};

		Schema wrongFormat = Schema.create(Schema.Type.BYTES);
		wrongFormat.addProp("format", "unknown");
		assertThatThrownBy(() -> parserBase.createBinaryResolver(wrongFormat)).isInstanceOf(ResolvingFailure.class);

		Schema fixedSchema = Schema.createFixed("size8", null, null, 8);
		fixedSchema.addProp("format", "base16");
		assertThatThrownBy(() -> parserBase.createBinaryResolver(fixedSchema).addContent(null, "010203"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Wrong input size: expected 8 bytes, but got 3 bytes");
	}

	@Test
	void testResolvingAllTypes() {
		Schema schema = new SchemaParser().parse("""
				{"type": "record", "name": "AllTypes", "fields": [
				    {"name": "optionalBoolean", "type": ["null", "boolean"], "aliases": ["bool"]},
				    {"name": "shortInt", "type": "int"},
				    {"name": "longInt", "type": "long"},
				    {"name": "singleFloat", "type": "float"},
				    {"name": "doubleFloat", "type": "double"},
				    {"name": "fixedPoint", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
				    {"name": "texts", "type": {"type": "array", "items":  "string"}},
				    {"name": "binaries", "type": {"type": "record", "name": "BinaryData", "fields": [
				        {"name": "fixedBase16", "type": {"type": "fixed", "name": "Base16_8", "size": 8, "format": "base16"}},
				        {"name": "fixedBase64", "type": {"type": "fixed", "name": "Base64_8", "size": 8, "format": "base64"}},
				        {"name": "base16", "type": {"type": "bytes", "format": "base16"}},
				        {"name": "base64", "type": {"type": "bytes", "format": "base64"}}
				    ]}},
				    {"name": "datesAndTimes", "type": {"type": "record", "name": "DatesAndTimes", "fields": [
				        {"name": "dateOnly", "type": {"type": "int", "logicalType": "date"}},
				        {"name": "timeMillis", "type": {"type": "int", "logicalType": "time-millis"}},
				        {"name": "timeMicros", "type": {"type": "long", "logicalType": "time-micros"}},
				        {"name": "timestampMillis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
				        {"name": "timestampMicros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
				        {"name": "localTimestampMillis", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
				        {"name": "localTimestampMicros", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
				    ]}},
				    {"name": "choice", "type": ["null", {"type": "enum", "name": "Answer", "symbols": ["maybe", "yes", "no"], "default": "maybe"}], "default": null}
				]}
				""").mainSchema();
		ValueResolver resolver = new AsAvroParserBase<>(GenericData.get(), null, schema, Set.of(), ZoneOffset.ofHours(0)) {
		}.getResolver();
		Object object = resolver.createCollector();
		object = resolveScalar(resolver, object, "bool", "true");
		object = resolveScalar(resolver, object, "shortInt", "42");
		object = resolveScalar(resolver, object, "longInt", "123456");
		object = resolveScalar(resolver, object, "singleFloat", "123.456");
		object = resolveScalar(resolver, object, "doubleFloat", "654.321");
		object = resolveScalar(resolver, object, "fixedPoint", "12345.67");

		ValueResolver textsResolver = resolver.resolve("texts");
		Object texts = textsResolver.createCollector();
		texts = resolveScalar(textsResolver, texts, "anything", "Hello");
		texts = resolveScalar(textsResolver, texts, "anything", "World!");
		object = resolver.addProperty(object, "texts", textsResolver.complete(texts));

		ValueResolver bytesResolver = resolver.resolve("binaries");
		Object binaries = bytesResolver.createCollector();
		binaries = resolveScalar(bytesResolver, binaries, "fixedBase16", "0001020304050607");
		binaries = resolveScalar(bytesResolver, binaries, "fixedBase64", "AAECAwQFBgc=");
		binaries = resolveScalar(bytesResolver, binaries, "base16", "000102030405060708090A0B0C0D0E0F");
		binaries = resolveScalar(bytesResolver, binaries, "base64", "AAECAwQFBgcICQoLDA0ODw==");
		object = resolver.addProperty(object, "binaries", bytesResolver.complete(binaries));

		ValueResolver dtResolver = resolver.resolve("datesAndTimes");
		Object datesAndTimes = dtResolver.createCollector();
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "dateOnly", "2023-04-17");
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "timeMillis", "17:08:34.567+02:00");
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "timeMicros", "17:08:34.567123");
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "timestampMillis", "2023-04-17T17:08:34.567123CET");
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "timestampMicros", "2023-04-17T17:08:34.567123+02:00");
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "localTimestampMillis", "2023-04-17T17:08:34.567");
		datesAndTimes = resolveScalar(dtResolver, datesAndTimes, "localTimestampMicros", "2023-04-17T17:08:34.567123");
		object = resolver.addProperty(object, "datesAndTimes", dtResolver.complete(datesAndTimes));

		object = resolveScalar(resolver, object, "choice", "yes");
		object = resolveScalar(resolver, object, "extra", "Ignored, as property does not exist");
		Object result = resolver.complete(object);

		assertThat(result.toString()).isEqualTo(
				"{" +
				"\"optionalBoolean\": true, " +
				"\"shortInt\": 42, " +
				"\"longInt\": 123456, " +
				"\"singleFloat\": 123.456, " +
				"\"doubleFloat\": 654.321, " +
				"\"fixedPoint\": 12345.67, " +
				"\"texts\": [\"Hello\", \"World!\"], " +
				"\"binaries\": {\"fixedBase16\": \"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\", " +
				"\"fixedBase64\": \"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\", " +
				"\"base16\": \"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u000F\", " +
				"\"base64\": \"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u000F\"}, " +
				"\"datesAndTimes\": {\"dateOnly\": \"2023-04-17\", " +
				"\"timeMillis\": \"17:08:34.567+02:00\", \"timeMicros\": \"17:08:34.567123Z\", " +
				"\"timestampMillis\": \"2023-04-17T15:08:34.567123Z\", \"timestampMicros\": \"2023-04-17T15:08:34.567123Z\", " +
				"\"localTimestampMillis\": \"2023-04-17T17:08:34.567\", \"localTimestampMicros\": \"2023-04-17T17:08:34.567123\"}, " +
				"\"choice\": \"yes\"" +
				"}");

		// Other ignored data (i.e., no-op parsing) just like "extra" above

		ValueResolver enumResolver = resolver.resolve("choice");
		ValueResolver noop = enumResolver.resolve("enumsDoNotHaveProperties");
		assertThat(noop).isSameAs(ValueResolver.NOOP);
		assertThat(noop.complete(noop.addContent(noop.createCollector(), "whatever"))).isNull();
		assertThat(enumResolver.complete(enumResolver.addProperty(enumResolver.createCollector(), "enumsDoNotHaveProperties", null))).isNull();
	}

	private Object resolveScalar(ValueResolver resolver, Object collector, String propertyName, String formattedValue) {
		ValueResolver propertyResolver = resolver.resolve(propertyName);
		Object parsedValue = propertyResolver.complete(propertyResolver.addContent(propertyResolver.createCollector(), formattedValue));
		return resolver.addProperty(collector, propertyName, parsedValue);
	}

	@Test
	void testFailuresForUnmatchedBinaryData() {
		Schema bytesSchema = Schema.create(Schema.Type.BYTES);
		assertThatThrownBy(() -> new AsAvroParserBase<>(GenericData.get(), null, bytesSchema, Set.of()) {
		}).isInstanceOf(ResolvingFailure.class);
	}

	@Test
	void testParsingInvalidEnum() {
		Schema enumWithDefault = Schema.createEnum("choice", null, null, List.of("maybe", "yes", "no"), "maybe");
		ValueResolver res1 = new AsAvroParserBase<>(GenericData.get(), null, enumWithDefault, Set.of()) {
		}.getResolver();
		Object result = res1.complete(res1.addContent(res1.createCollector(), "invalid"));
		assertThat(result).isInstanceOf(GenericData.EnumSymbol.class);
		assertThat(result.toString()).isEqualTo("maybe");

		Schema enumWithoutDefault = Schema.createEnum("choice", null, null, List.of("maybe", "yes", "no"));
		ValueResolver res2 = new AsAvroParserBase<>(GenericData.get(), null, enumWithoutDefault, Set.of()) {
		}.getResolver();
		assertThatThrownBy(() -> res2.complete(res2.addContent(res2.createCollector(), "invalid"))).isInstanceOf(NullPointerException.class);
	}

	@Test
	void coverMethodThatCannotBeCalled() {
		// There is no code path that actively causes this failure (that would mean a bug in building resolvers).
		assertThatThrownBy(() -> new ValueResolver() {
		}.addContent(null, null)).isInstanceOf(IllegalStateException.class);
	}

	@Test
	void testRecordImplicitArrayFields() {
		ValueResolver sr = new ScalarValueResolver(s -> s);
		Schema.Field f = new Schema.Field("texts", Schema.createArray(Schema.create(Schema.Type.STRING)));
		RecordResolver rr = new RecordResolver(GenericData.get(), Schema.createRecord("Record", null, null, false, List.of(f)));
		rr.addArrayResolver("texts", f, sr);

		Object result = rr.createCollector();
		result = resolveScalar(rr, result, "texts", "Hello");
		result = resolveScalar(rr, result, "texts", "World!");
		result = rr.complete(result);
		assertThat(result.toString()).isEqualTo("{\"texts\": [\"Hello\", \"World!\"]}");
	}

	@Test
	void testRecordFieldDefaultValues() {
		ValueResolver sr = new ScalarValueResolver(s -> s);
		Schema optionalString = Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL));
		Schema.Field f = new Schema.Field("value", optionalString, null, "missing");
		RecordResolver rr = new RecordResolver(GenericData.get(), Schema.createRecord("Record", null, null, false, List.of(f)));
		rr.addResolver("value", f, sr);

		Object result1 = rr.complete(rr.createCollector());
		assertThat(result1.toString()).isEqualTo("{\"value\": \"missing\"}");

		Object result2 = rr.createCollector();
		ValueResolver vr = rr.resolve("value");
		result2 = rr.addProperty(result2, "value", vr.complete(vr.addContent(vr.createCollector(), null)));
		result2 = rr.complete(result2);
		assertThat(result2.toString()).isEqualTo("{\"value\": null}");
		assertThat(result2).isNotEqualTo(result1);
	}

	@Test
	void testRecordContentField() {
		ValueResolver sr = new ScalarValueResolver(s -> s);
		Schema.Field f = new Schema.Field("value", Schema.create(Schema.Type.STRING), null, "missing");
		RecordResolver rr = new RecordResolver(GenericData.get(), Schema.createRecord("Record", null, null, false, List.of(f)));
		rr.addResolver("value", f, sr);

		Object result = rr.createCollector();
		result = rr.addContent(result, "present");
		result = rr.complete(result);
		assertThat(result.toString()).isEqualTo("{\"value\": \"present\"}");
	}

	@Test
	void testValueResolverContentParseFlag() {
		ValueResolver resolver = new ValueResolver() {
		};
		assertThat(resolver.parseContent()).isTrue();
		resolver.doNotParseContent();
		assertThat(resolver.parseContent()).isFalse();
	}
}
