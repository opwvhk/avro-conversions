package opwvhk.avro.xml;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;

/**
 * Additional Avro conversions.
 */
public final class AvroConversions {
	/**
	 * Convert an Avro logical type "{@link LogicalTypes.TimeMillis time-millis}" to/from the number of milliseconds since midnight in the specified timezone.
	 * If not specified, the timezone {@link ZoneOffset#UTC UTC} is used.
	 */
	public static class OffsetTimeMillisConversion extends Conversion<OffsetTime> {
		/**
		 * Property used to specify the timezone to use for the time stored in an Avro record. This is also the timezone used when deserializing an Avro
		 * record.
		 */
		public static final String TIMEZONE_PROP = "timezone";
		private static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.UTC;

		@Override
		public Class<OffsetTime> getConvertedType() {
			return OffsetTime.class;
		}

		@Override
		public String getLogicalTypeName() {
			return new TimeConversions.TimeMillisConversion().getLogicalTypeName();
		}

		@Override
		public OffsetTime fromInt(Integer value, Schema schema, LogicalType type) {
			LocalTime localTime = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value));
			ZoneOffset zone = zoneFor(schema);
			return localTime.atOffset(zone);
		}

		@Override
		public Integer toInt(OffsetTime value, Schema schema, LogicalType type) {
			ZoneOffset zone = zoneFor(schema);
			OffsetTime timeAtOffset = value.withOffsetSameInstant(zone);
			LocalTime localTime = timeAtOffset.toLocalTime();
			return (int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay());
		}

		private static ZoneOffset zoneFor(Schema schema) {
			return Optional.ofNullable(schema.getProp(TIMEZONE_PROP)).map(ZoneOffset::of).orElse(DEFAULT_ZONE_OFFSET);
		}

		@Override
		public Schema getRecommendedSchema() {
			Schema schema = new TimeConversions.TimeMillisConversion().getRecommendedSchema();
			schema.addProp(TIMEZONE_PROP, DEFAULT_ZONE_OFFSET.toString());
			return schema;
		}
	}
	/**
	 * Convert an Avro logical type "{@link LogicalTypes.TimeMicros time-micros}" to/from the number of microseconds since midnight in the specified timezone.
	 * If not specified, the timezone {@link ZoneOffset#UTC UTC} is used.
	 */
	public static class OffsetTimeMicrosConversion extends Conversion<OffsetTime> {
		/**
		 * Property used to specify the timezone to use for the time stored in an Avro record. This is also the timezone used when deserializing an Avro
		 * record.
		 */
		public static final String TIMEZONE_PROP = "timezone";
		private static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.UTC;

		@Override
		public Class<OffsetTime> getConvertedType() {
			return OffsetTime.class;
		}

		@Override
		public String getLogicalTypeName() {
			return new TimeConversions.TimeMicrosConversion().getLogicalTypeName();
		}

		@Override
		public OffsetTime fromLong(Long value, Schema schema, LogicalType type) {
			LocalTime localTime = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(value));
			ZoneOffset zone = zoneFor(schema);
			return localTime.atOffset(zone);
		}

		@Override
		public Long toLong(OffsetTime value, Schema schema, LogicalType type) {
			ZoneOffset zone = zoneFor(schema);
			OffsetTime timeAtOffset = value.withOffsetSameInstant(zone);
			LocalTime localTime = timeAtOffset.toLocalTime();
			return TimeUnit.NANOSECONDS.toMicros(localTime.toNanoOfDay());
		}

		private static ZoneOffset zoneFor(Schema schema) {
			return Optional.ofNullable(schema.getProp(TIMEZONE_PROP)).map(ZoneOffset::of).orElse(DEFAULT_ZONE_OFFSET);
		}

		@Override
		public Schema getRecommendedSchema() {
			Schema schema = new TimeConversions.TimeMicrosConversion().getRecommendedSchema();
			schema.addProp(TIMEZONE_PROP, DEFAULT_ZONE_OFFSET.toString());
			return schema;
		}
	}

	private AvroConversions() {
		// Utility class: do not instantiate.
	}
}
