package opwvhk.avro.xml.datamodel;

public sealed interface ScalarType extends Type
		permits FixedType, DecimalType, EnumType {
	default Object parse(String text) {
		return text == null ? null : parseNonNull(text);
	}

	default Object parseNonNull(String text) {
		throw new UnsupportedOperationException("Default values for this type are not supported.");
	}
}
