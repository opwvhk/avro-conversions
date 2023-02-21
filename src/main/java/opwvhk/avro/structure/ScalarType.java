package opwvhk.avro.structure;

public sealed interface ScalarType extends Type
		permits FixedType, DecimalType, EnumType {
	default Object parse(String text) {
		return text == null ? null : parseNonNull(text);
	}

	Object parseNonNull(String text);
}
