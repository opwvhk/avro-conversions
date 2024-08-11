package opwvhk.avro.xml.datamodel;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Base16;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Locale;

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
		public Boolean parseNonNull(String text) {
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
		public Float parseNonNull(String text) {
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
		public Double parseNonNull(String text) {
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
		public String parseNonNull(String text) {
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

		private static final Base16 BASE_16 = new Base16();

		@Override
		public ByteBuffer parseNonNull(String text) {
			return ByteBuffer.wrap(BASE_16.decode(text.toUpperCase(Locale.ROOT)));
		}

		@Override
		public Schema toSchema() {
			Schema schema = Schema.create(BYTES);
			schema.addProp("format", "base16");
			return schema;
		}
	},
	/**
	 * Binary data, encoded as base64.
	 */
	BINARY_BASE64() {
		@Override
		public ByteBuffer parseNonNull(String text) {
			return ByteBuffer.wrap(Base64.getDecoder().decode(text));
		}

		@Override
		public Schema toSchema() {
			Schema schema = Schema.create(BYTES);
			schema.addProp("format", "base64");
			return schema;
		}
	};

	@Override
	public String debugString(String indent) {
		return indent + name().toLowerCase(Locale.ROOT);
	}
}
