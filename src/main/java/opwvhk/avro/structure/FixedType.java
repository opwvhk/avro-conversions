package opwvhk.avro.structure;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
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
			return LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
		}
	}, DATETIME() {
		@Override
		public Object parseNonNull(String text) {
			return Instant.parse(text).toEpochMilli();
		}
	}, TIME() {
		@Override
		public Object parseNonNull(String text) {
			return LocalTime.parse(text, DateTimeFormatter.ISO_LOCAL_TIME).toNanoOfDay() / 1_000_000L;
		}
	}, DATETIME_MICROS() {
		@Override
		public Object parseNonNull(String text) {
			Instant instant = Instant.parse(text);
			return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
		}
	}, TIME_MICROS() {
		@Override
		public Object parseNonNull(String text) {
			return LocalTime.parse(text, DateTimeFormatter.ISO_LOCAL_TIME).toNanoOfDay() / 1_000L;
		}
	}, STRING() {
		@Override
		public Object parseNonNull(String text) {
			return text;
		}
	}, BINARY_HEX() {
		@Override
		public Object parseNonNull(String text) {
			return new BigInteger(text, 16).toByteArray();
		}
	}, BINARY_BASE64() {
		@Override
		public Object parseNonNull(String text) {
			return Base64.getDecoder().decode(text);
		}
	};

	@Override
	public String debugString(String indent) {
		return indent + name().toLowerCase(Locale.ROOT);
	}
}
