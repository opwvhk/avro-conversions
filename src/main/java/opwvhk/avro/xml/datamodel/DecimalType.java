package opwvhk.avro.xml.datamodel;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;

/**
 * A number type with fixed point, including integer numbers.
 *
 * @param bitSize   the bit size of the integer number ({@link Integer#MAX_VALUE} for fractional numbers)
 * @param precision the number of digits in the number
 * @param scale     the scale of the number (number of digits after the decimal point)
 */
public record DecimalType(int bitSize, int precision, int scale) implements ScalarType {
	/**
	 * Constant denoting a 32-bit integer number.
	 */
	public static final DecimalType INTEGER_TYPE = DecimalType.integer(Integer.SIZE, Integer.toString(Integer.MAX_VALUE).length());
	/**
	 * Constant denoting a 64-bit integer number.
	 */
	public static final DecimalType LONG_TYPE = DecimalType.integer(Long.SIZE, Long.toString(Long.MAX_VALUE).length());

	/**
	 * Create a {@code DecimalType} for a fractional number.
	 *
	 * @param precision the precision of the number
	 * @param scale     the scale of the number
	 * @return the {@code DecimalType}
	 */
	public static DecimalType withFraction(int precision, int scale) {
		return new DecimalType(Integer.MAX_VALUE, precision, scale);
	}

	/**
	 * Create a {@code DecimalType} for an integer number.
	 *
	 * @param bitSize   the bit size of the number
	 * @param precision the precision of the number
	 * @return the {@code DecimalType}
	 */
	public static DecimalType integer(int bitSize, int precision) {
		return new DecimalType(bitSize, precision, 0);
	}

	/**
	 * Create a {@code DecimalType} for a fractional number.
	 *
	 * @param bitSize   the bit size of the number
	 * @param precision the precision of the number
	 * @param scale     the scale of the number
	 */
	public DecimalType {
		if (scale == 0) {
			if (bitSize <= 0) {
				throw new IllegalArgumentException("bitSize must be positive");
			}
		} else if (bitSize != Integer.MAX_VALUE) {
			throw new IllegalArgumentException(
					"Either scale needs to be 0 (for integer numbers), or bitSize needs to be Integer.MAX_VALUE (for fractional numbers)");
		}
	}

	@Override
	public Object parseNonNull(String text) {
		if (bitSize < Integer.SIZE) {
			return Integer.decode(text);
		} else if (bitSize < Long.SIZE) {
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
	public Schema toSchema() {
		if (bitSize <= Integer.SIZE) {
			return Schema.create(INT);
		} else if (bitSize <= Long.SIZE) {
			return Schema.create(LONG);
		} else {
			return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(BYTES));
		}
	}

	@Override
	public String debugString(String indent) {
		String type = scale() == 0 ? "decimal(%d; %d bits)".formatted(precision, bitSize) : "decimal(%d,%d)".formatted(precision, scale);
		return indent + type;
	}
}
