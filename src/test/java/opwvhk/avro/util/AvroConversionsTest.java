package opwvhk.avro.util;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.junit.jupiter.api.Test;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.*;

class AvroConversionsTest {
	@Test
    void testTimeMillisBasics() {
		Conversion<LocalTime> localTimeConversion  = new TimeConversions.TimeMillisConversion();
		Conversion<OffsetTime> offsetTimeConversion  = new AvroConversions.OffsetTimeMillisConversion();
		assertThat(offsetTimeConversion.getLogicalTypeName()).isEqualTo(localTimeConversion.getLogicalTypeName());

		Schema schema = localTimeConversion.getRecommendedSchema();
		schema.addProp(AvroConversions.OffsetTimeMillisConversion.TIMEZONE_PROP, UTC.toString());
		assertThat(offsetTimeConversion.getRecommendedSchema()).isEqualTo(schema);

		assertThat(offsetTimeConversion.getConvertedType()).isEqualTo(OffsetTime.class);
	}

	@Test
    void testTimeMillisConversion() {
		Conversion<LocalTime> localTimeConversion  = new TimeConversions.TimeMillisConversion();
		Conversion<OffsetTime> offsetTimeConversion  = new AvroConversions.OffsetTimeMillisConversion();

		Schema schema = localTimeConversion.getRecommendedSchema();
		ZoneOffset offset = ZoneOffset.of("+01:00");
		schema.addProp(AvroConversions.OffsetTimeMillisConversion.TIMEZONE_PROP, offset.getId());

		OffsetTime source = OffsetTime.of(13, 24, 56, 123_456_789, UTC);

		Integer rawSource = toRaw(source, schema, offsetTimeConversion);
		assertThat(rawSource).isEqualTo(51_896_123);

		LocalTime localTime = toLogical(rawSource, schema, localTimeConversion);
		assertThat(localTime).isEqualTo(LocalTime.of(14,24,56,123_000_000));

		OffsetTime offsetTime = toLogical(rawSource, schema, offsetTimeConversion);
		assertThat(offsetTime).isEqualTo(OffsetTime.of(14,24,56,123_000_000, offset));
		assertThat(offsetTime.withOffsetSameInstant(UTC)).isEqualTo(source.truncatedTo(ChronoUnit.MILLIS));
	}

	@Test
    void testTimeMicrosBasics() {
		Conversion<LocalTime> localTimeConversion  = new TimeConversions.TimeMicrosConversion();
		Conversion<OffsetTime> offsetTimeConversion  = new AvroConversions.OffsetTimeMicrosConversion();
		assertThat(offsetTimeConversion.getLogicalTypeName()).isEqualTo(localTimeConversion.getLogicalTypeName());

		Schema schema = localTimeConversion.getRecommendedSchema();
		schema.addProp(AvroConversions.OffsetTimeMicrosConversion.TIMEZONE_PROP, UTC.toString());
		assertThat(offsetTimeConversion.getRecommendedSchema()).isEqualTo(schema);

		assertThat(offsetTimeConversion.getConvertedType()).isEqualTo(OffsetTime.class);
	}

	@Test
    void testTimeMicrosConversion() {
		Conversion<LocalTime> localTimeConversion  = new TimeConversions.TimeMicrosConversion();
		Conversion<OffsetTime> offsetTimeConversion  = new AvroConversions.OffsetTimeMicrosConversion();

		assertThat(offsetTimeConversion.getLogicalTypeName()).isEqualTo(localTimeConversion.getLogicalTypeName());

		Schema schema = localTimeConversion.getRecommendedSchema();
		ZoneOffset offset = ZoneOffset.of("+01:00");
		schema.addProp(AvroConversions.OffsetTimeMicrosConversion.TIMEZONE_PROP, offset.getId());

		OffsetTime source = OffsetTime.of(13, 24, 56, 123_456_789, UTC);

		Long rawSource = toRaw(source, schema, offsetTimeConversion);
		assertThat(rawSource).isEqualTo(51_896_123_456L);

		LocalTime localTime = toLogical(rawSource, schema, localTimeConversion);
		assertThat(localTime).isEqualTo(LocalTime.of(14,24,56,123_456_000));

		OffsetTime offsetTime = toLogical(rawSource, schema, offsetTimeConversion);
		assertThat(offsetTime).isEqualTo(OffsetTime.of(14,24,56,123_456_000, offset));
		assertThat(offsetTime.withOffsetSameInstant(UTC)).isEqualTo(source.truncatedTo(ChronoUnit.MICROS));
	}

	private static <R, L> R toRaw(L datum, Schema schema, Conversion<L> conversion) {
		LogicalType logicalType = LogicalTypes.fromSchema(schema);
		return (R) Conversions.convertToRawType(datum, schema, logicalType, conversion);
	}

	private static <L> L toLogical(Object datum, Schema schema, Conversion<L> conversion) {
		LogicalType logicalType = LogicalTypes.fromSchema(schema);
		return (L) Conversions.convertToLogicalType(datum, schema, logicalType, conversion);
	}
}
