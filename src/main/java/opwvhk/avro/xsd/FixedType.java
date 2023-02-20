package opwvhk.avro.xsd;

import java.util.Locale;

public enum FixedType implements ScalarType {
	BOOLEAN, FLOAT, DOUBLE, DATE, DATETIME, TIME, STRING, BINARY_HEX, BINARY_BASE64;

	@Override
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}
}
