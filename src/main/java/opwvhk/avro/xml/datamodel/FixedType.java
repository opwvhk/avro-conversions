package opwvhk.avro.xml.datamodel;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;

public enum FixedType implements ScalarType {

	BOOLEAN() {
		@Override
		public Object parseNonNull(String text) {
			return Boolean.valueOf(text);
		}
	}, FLOAT() {
		@Override
		public Object parseNonNull(String text) {
			return Float.valueOf(text);
		}
	}, DOUBLE() {
		@Override
		public Object parseNonNull(String text) {
			return Double.valueOf(text);
		}
	}, DATE() {
		@Override
		public Object parseNonNull(String text) {
			return LocalDate.parse(text, DATE_FORMAT);
		}
	}, DATETIME() {
		@Override
		public Object parseNonNull(String text) {
			ZonedDateTime dateTime = ZonedDateTime.parse(text, DATE_TIME_FORMAT);
			return dateTime.toInstant().truncatedTo(ChronoUnit.MILLIS);
		}
	}, TIME() {
		@Override
		public Object parseNonNull(String text) {
			return LocalTime.parse(text, TIME_FORMAT).truncatedTo(ChronoUnit.MILLIS);
		}
	}, DATETIME_MICROS() {
		@Override
		public Object parseNonNull(String text) {
			ZonedDateTime dateTime = ZonedDateTime.parse(text, DATE_TIME_FORMAT);
			return dateTime.toInstant().truncatedTo(ChronoUnit.MICROS);
		}
	}, TIME_MICROS() {
		@Override
		public Object parseNonNull(String text) {
			return LocalTime.parse(text, TIME_FORMAT).truncatedTo(ChronoUnit.MICROS);
		}
	}, STRING() {
		@Override
		public Object parseNonNull(String text) {
			return text;
		}
	}, BINARY_HEX() {
		@Override
		public Object parseNonNull(String text) {
			return ByteBuffer.wrap(new BigInteger(text, 16).toByteArray());
		}
	}, BINARY_BASE64() {
		@Override
		public Object parseNonNull(String text) {
			return ByteBuffer.wrap(Base64.getDecoder().decode(text));
		}
	};

	@Override
	public String debugString(String indent) {
		return indent + name().toLowerCase(Locale.ROOT);
	}

	public static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4)
			.appendLiteral("-")
			.appendValue(ChronoField.MONTH_OF_YEAR, 2)
			.appendLiteral("-")
			.appendValue(ChronoField.DAY_OF_MONTH, 2)
			.toFormatter(Locale.ROOT);
	public static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.appendValue(ChronoField.HOUR_OF_DAY, 2)
			.appendLiteral(":")
			.appendValue(ChronoField.MINUTE_OF_HOUR, 2)
			.appendLiteral(":")
			.appendValue(ChronoField.SECOND_OF_MINUTE, 2)
			.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
			.optionalStart()
			.appendZoneOrOffsetId()
			.optionalEnd()
			.toFormatter(Locale.ROOT);
	public static final DateTimeFormatter DATE_TIME_FORMAT = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.append(DATE_FORMAT)
			.appendLiteral("T")
			.append(TIME_FORMAT)
			.toFormatter(Locale.ROOT);
}
