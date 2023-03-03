package opwvhk.avro.xml.datamodel;

import java.math.BigDecimal;
import java.math.RoundingMode;

public record DecimalType(int bitSize, int precision, int scale) implements ScalarType {
	public static final DecimalType INTEGER_TYPE = DecimalType.integer(Integer.SIZE, Integer.toString(Integer.MAX_VALUE).length());
	public static final DecimalType LONG_TYPE = DecimalType.integer(Long.SIZE, Long.toString(Long.MAX_VALUE).length());

	public static DecimalType withFraction(int precision, int scale) {
		return new DecimalType(-1, precision, scale);
	}

	public static DecimalType integer(int bitSize, int precision) {
		return new DecimalType(bitSize, precision, 0);
	}

	public DecimalType {
		if (scale == 0) {
			if (bitSize <= 0) {
				throw new IllegalArgumentException("bitSize must be positive");
			}
		} else if (bitSize != -1) {
			throw new IllegalArgumentException("Either scale needs to be 0 (for integer numbers), or bitSize needs to be -1 (for fractional numbers)");
		}
	}

	@Override
	public Object parseNonNull(String text) {
		if (scale == 0 && bitSize < Integer.SIZE) {
			return Integer.decode(text);
		} else if (scale == 0 && bitSize < Long.SIZE) {
			return Long.decode(text);
		} else {
			BigDecimal decimal = new BigDecimal(text).setScale(scale, RoundingMode.UNNECESSARY);
			if (decimal.precision() > precision) {
				throw new ArithmeticException("Decimal value larger than supported precision");
			}
			return decimal;
		}
	}

	@Override
	public String debugString(String indent) {
		String type = scale() == 0 ? "decimal(%d; %d bits)".formatted(precision, bitSize) : "decimal(%d,%d)".formatted(precision, scale);
		return indent + type;
	}
}
