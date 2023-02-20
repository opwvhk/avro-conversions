package opwvhk.avro.xsd;

public record DecimalType(int bitSize, int precision, int scale) implements ScalarType {
	static final DecimalType INTEGER_TYPE = DecimalType.integer(Integer.SIZE, Integer.toString(Integer.MAX_VALUE).length());
	static final DecimalType LONG_TYPE = DecimalType.integer(Long.SIZE, Long.toString(Long.MAX_VALUE).length());

	static DecimalType withFraction(int precision, int scale) {
		return new DecimalType(-1, precision, scale);
	}

	static DecimalType integer(int bitSize, int precision) {
		return new DecimalType(bitSize, precision, 0);
	}

	@Override
	public String toString() {
		return scale() == 0 ? "decimal(%d; %d bits)".formatted(precision, bitSize) : "decimal(%d,%d)".formatted(precision, scale);
	}
}
