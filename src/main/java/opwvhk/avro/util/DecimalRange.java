package opwvhk.avro.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.math.RoundingMode.DOWN;
import static java.math.RoundingMode.UNNECESSARY;

/**
 * A range of decimal numbers.
 *
 * @param lowerBound          the lower bound of the range, if any
 * @param lowerBoundInclusive whether he lower bound is inclusive ({@code true}) or exclusive ({@code false})
 * @param upperBound          the upper bound of the range, if any
 * @param upperBoundInclusive whether he upper bound is inclusive ({@code true}) or exclusive ({@code false})
 */
public record DecimalRange(BigDecimal lowerBound, boolean lowerBoundInclusive, BigDecimal upperBound, boolean upperBoundInclusive) {
    /**
     * Create a decimal range.
     *
     * @param lowerBound          the lower bound of the range, if any
     * @param lowerBoundInclusive whether he lower bound is inclusive ({@code true}) or exclusive ({@code false})
     * @param upperBound          the upper bound of the range, if any
     * @param upperBoundInclusive whether he upper bound is inclusive ({@code true}) or exclusive ({@code false})
     */
    public DecimalRange {
        if (lowerBound == null && lowerBoundInclusive || upperBound == null && upperBoundInclusive) {
            throw new IllegalArgumentException("Illegal range: a null bounds cannot be inclusive");
        }
        if (lowerBound != null && upperBound != null) {
            int cmp = lowerBound.compareTo(upperBound);
            if (lowerBoundInclusive && upperBoundInclusive ? cmp > 0 : cmp >= 0) {
                throw new IllegalArgumentException("Illegal range: lower bound must be smaller than upper bound, or equal if both bounds are inclusive");
            }
        }
    }

    /**
     * Create a decimal range using both inclusive and exclusive bounds. Inclusive bounds take precedence.
     *
     * @param lowerBoundInclusive the inclusive lower bound of this range
     * @param lowerBoundExclusive the exclusive lower bound of this range; used if no inclusive lower bound is given
     * @param upperBoundInclusive the inclusive upper bound of this range
     * @param upperBoundExclusive the exclusive upper bound of this range; used if no inclusive upper bound is given
     */
    public DecimalRange(BigDecimal lowerBoundInclusive, BigDecimal lowerBoundExclusive, BigDecimal upperBoundInclusive, BigDecimal upperBoundExclusive) {
        this(lowerBoundInclusive == null ? lowerBoundExclusive : lowerBoundInclusive, lowerBoundInclusive != null,
                upperBoundInclusive == null ? upperBoundExclusive : upperBoundInclusive, upperBoundInclusive != null);
    }

    /**
     * Return a range that (just) includes this range and the other range. Treats {@code null} bounds as positive or negative infinity.
     *
     * @param other another range
     * @return a new range, that has the minimum/maximum of the lower/upper bounds of the ranges as lower/upper bounds
     */
    public DecimalRange extendWith(DecimalRange other) {
        BigDecimal lowerBound;
        boolean includeLowerBound;
        int cmp = compare(this.lowerBound, other.lowerBound, NullHandling.NULL_AS_NEGATIVE_INFINITY);
        if (cmp < 0) {
            lowerBound = this.lowerBound;
            includeLowerBound = this.lowerBoundInclusive;
        } else if (cmp == 0) {
            if (this.lowerBound == null /* && other.lowerBound == null */) {
                lowerBound = null;
            } else if (this.lowerBound.scale() > other.lowerBound.scale()) {
                lowerBound = this.lowerBound;
            } else {
                lowerBound = other.lowerBound;
            }
            includeLowerBound = this.lowerBoundInclusive | other.lowerBoundInclusive;
        } else /* cmp > 0 */ {
            lowerBound = other.lowerBound;
            includeLowerBound = other.lowerBoundInclusive;
        }

        BigDecimal upperBound;
        boolean includeUpperBound;
        cmp = compare(this.upperBound, other.upperBound, NullHandling.NULL_AS_POSITIVE_INFINITY);
        if (cmp < 0) {
            upperBound = other.upperBound;
            includeUpperBound = other.upperBoundInclusive;
        } else if (cmp == 0) {
            if (this.upperBound == null /* && other.upperBound == null */) {
                upperBound = null;
            } else if (this.upperBound.scale() > other.upperBound.scale()) {
                upperBound = this.upperBound;
            } else {
                upperBound = other.upperBound;
            }
            includeUpperBound = this.upperBoundInclusive | other.upperBoundInclusive;
        } else /* cmp > 0 */ {
            upperBound = this.upperBound;
            includeUpperBound = this.upperBoundInclusive;
        }
        return new DecimalRange(lowerBound, includeLowerBound, upperBound, includeUpperBound);
    }

    /**
     * Return a range that (just) includes this range and the other range. Treats {@code null} bounds as positive or negative infinity.
     *
     * @param other another range
     * @return a new range, that has the maximum/minimum of the lower/upper bounds of the ranges as lower/upper bounds
     */
    public DecimalRange restrictTo(DecimalRange other) {
        BigDecimal lowerBound;
        boolean includeLowerBound;
        int cmp = compare(this.lowerBound, other.lowerBound, NullHandling.NULL_AS_NEGATIVE_INFINITY);
        if (cmp < 0) {
            lowerBound = other.lowerBound;
            includeLowerBound = other.lowerBoundInclusive;
        } else if (cmp == 0) {
            if (this.lowerBound == null /* && other.lowerBound == null */) {
                lowerBound = null;
            } else if (this.lowerBound.scale() > other.lowerBound.scale()) {
                lowerBound = this.lowerBound;
            } else {
                lowerBound = other.lowerBound;
            }
            includeLowerBound = this.lowerBoundInclusive & other.lowerBoundInclusive;
        } else /* cmp > 0 */ {
            lowerBound = this.lowerBound;
            includeLowerBound = this.lowerBoundInclusive;
        }

        BigDecimal upperBound;
        boolean includeUpperBound;
        cmp = compare(this.upperBound, other.upperBound, NullHandling.NULL_AS_POSITIVE_INFINITY);
        if (cmp < 0) {
            upperBound = this.upperBound;
            includeUpperBound = this.upperBoundInclusive;
        } else if (cmp == 0) {
            if (this.upperBound == null /* && other.upperBound == null */) {
                upperBound = null;
            } else if (this.upperBound.scale() > other.upperBound.scale()) {
                upperBound = this.upperBound;
            } else {
                upperBound = other.upperBound;
            }
            includeUpperBound = this.upperBoundInclusive & other.upperBoundInclusive;
        } else /* cmp > 0 */ {
            upperBound = other.upperBound;
            includeUpperBound = other.upperBoundInclusive;
        }
        return new DecimalRange(lowerBound, includeLowerBound, upperBound, includeUpperBound);
    }

	/**
	 * Determine whether the range is bounded. A range is bounded if there is both a lower and upper bound.
	 *
	 * @return {@code true} if this range is bounded
	 */
	public boolean isBounded() {
		return lowerBound != null && upperBound != null;
	}

    /**
     * Determine whether the range can be an integer range.
     *
     * @param allowDecimalPlaces whether zero fractions are allowed
     * @return {@code true} if the bounds of this range are whole numbers, with an optional zero fraction if allowed
     */
    public boolean isIntegerRange(boolean allowDecimalPlaces) {
        Predicate<BigDecimal> test = allowDecimalPlaces ? bd -> bd.compareTo(bd.setScale(0, DOWN)) == 0 : bd -> bd.scale() <= 0;
        return Stream.of(lowerBound, upperBound).filter(Objects::nonNull).allMatch(test);
    }

    /**
     * Calculate the number of bits needed to represent numbers in this range. Assumes the range is a closed range.
     *
     * @return the number of bits needed to represent numbers in this range, or 0 if unknown
     */
    public int integerBitSize() {
        return Stream.of(lowerBound, upperBound).filter(Objects::nonNull)
                       .map(bd -> bd.setScale(0, DOWN))
                       .map(BigDecimal::toBigInteger)
                       .map(BigInteger::abs)
                       .mapToInt(BigInteger::bitLength)
                       .max()
                       .orElse(-1) + 1;
    }

    /**
     * Calculate the maximum number of digits of numbers in this range.
     *
     * @return the maximum number of digits of numbers in this range
     */
    public int requiredPrecision() {
        int requiredScale = requiredScale();
        return Stream.of(lowerBound, upperBound).filter(Objects::nonNull)
                .map(bd -> bd.setScale(requiredScale, UNNECESSARY))
                .mapToInt(BigDecimal::precision)
                .max().orElse(0);
    }

    /**
     * Calculate the maximum number of digits of numbers in this range.
     *
     * @return the maximum number of digits of numbers in this range
     */
    public int requiredScale() {
        return Stream.of(lowerBound, upperBound).filter(Objects::nonNull)
                .mapToInt(BigDecimal::scale)
                .max().orElse(0);
    }

    @Override
    public String toString() {
        return (lowerBoundInclusive ? "[" : "(") + (lowerBound == null ? "-inf" : lowerBound) + ", " +
               (upperBound == null ? "inf" : upperBound) + (upperBoundInclusive ? "]" : ")");
    }

    private static int compare(BigDecimal bd1, BigDecimal bd2, NullHandling nullHandling) {
        if (bd1 == null && bd2 == null) {
            return 0;
        } else if (bd1 == null) {
            return nullHandling == NullHandling.NULL_AS_NEGATIVE_INFINITY ? -1 : 1;
        } else if (bd2 == null) {
            return nullHandling == NullHandling.NULL_AS_POSITIVE_INFINITY ? -1 : 1;
        } else {
            return bd1.compareTo(bd2);
        }
    }

    private enum NullHandling {
        NULL_AS_POSITIVE_INFINITY, NULL_AS_NEGATIVE_INFINITY
    }
}
