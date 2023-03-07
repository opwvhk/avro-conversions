package opwvhk.avro.xml.datamodel;

import java.math.BigInteger;
import java.nio.ByteBuffer;
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
	}, DATE, DATETIME, TIME, STRING() {
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
}
