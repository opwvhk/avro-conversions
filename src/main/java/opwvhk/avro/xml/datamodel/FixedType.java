package opwvhk.avro.xml.datamodel;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Locale;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;

/**
 * {@code ScalarType} implementations without parameters.
 */
public enum FixedType implements ScalarType {

	/**
	 * A boolean value.
	 */
	BOOLEAN() {
		@Override
		public Object parseNonNull(String text) {
			return Boolean.valueOf(text);
		}

		@Override
		public Schema toSchema() {
			return Schema.create(Schema.Type.BOOLEAN);
		}
	},
	/**
	 * A floating point value (low precision).
	 */
	FLOAT() {
		@Override
		public Object parseNonNull(String text) {
			return Float.valueOf(text);
		}

		@Override
		public Schema toSchema() {
			return Schema.create(Schema.Type.FLOAT);
		}
	},
	/**
	 * A floating point value (high precision).
	 */
	DOUBLE() {
		@Override
		public Object parseNonNull(String text) {
			return Double.valueOf(text);
		}

		@Override
		public Schema toSchema() {
			return Schema.create(Schema.Type.DOUBLE);
		}
	},
	/**
	 * A date value.
	 */
	DATE() {
		@Override
		public Schema toSchema() {
			return LogicalTypes.date().addToSchema(Schema.create(INT));
		}
	},
	/**
	 * A timestamp value.
	 */
	DATETIME() {
		@Override
		public Schema toSchema() {
			return LogicalTypes.timestampMillis().addToSchema(Schema.create(LONG));
		}
	},
	/**
	 * A time value.
	 */
	TIME() {
		@Override
		public Schema toSchema() {
			return LogicalTypes.timeMillis().addToSchema(Schema.create(INT));
		}
	},
	/**
	 * A bit of text.
	 */
	STRING() {
		@Override
		public Object parseNonNull(String text) {
			return text;
		}

		@Override
		public Schema toSchema() {
			return Schema.create(Schema.Type.STRING);
		}
	},
	/**
	 * Binary data, encoded as hexadecimal bytes.
	 */
	BINARY_HEX() {
		@Override
		public Object parseNonNull(String text) {
			return ByteBuffer.wrap(new BigInteger(text, 16).toByteArray());
		}

		@Override
		public Schema toSchema() {
			return Schema.create(BYTES);
		}
	},
	/**
	 * Binary data, encoded as base64.
	 */
	BINARY_BASE64() {
		@Override
		public Object parseNonNull(String text) {
			return ByteBuffer.wrap(Base64.getDecoder().decode(text));
		}

		@Override
		public Schema toSchema() {
			return Schema.create(BYTES);
		}
	};

	@Override
	public String debugString(String indent) {
		return indent + name().toLowerCase(Locale.ROOT);
	}
}
