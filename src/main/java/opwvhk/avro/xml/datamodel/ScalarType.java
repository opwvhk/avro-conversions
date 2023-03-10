package opwvhk.avro.xml.datamodel;

/**
 * A type descriptor for scalar values.
 */
public sealed interface ScalarType extends Type
		permits FixedType, DecimalType, EnumType {
	/**
	 * Parse the text as value. This method is null-safe.
	 *
	 * @param text the text to parse
	 * @return the parsed value
	 */
	default Object parse(String text) {
		return text == null ? null : parseNonNull(text);
	}

	/**
	 * Parse the (non-{@code null}) text as value.
	 *
	 * @param text the text to parse (not {@code null})
	 * @return the parsed value
	 */
	default Object parseNonNull(String text) {
		throw new UnsupportedOperationException("Default values for this type are not supported.");
	}
}
