package opwvhk.avro.util;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DecimalRangeTest {
	private static final BigDecimal ONE_AND_A_HALF = new BigDecimal("1.5");
	private static final BigDecimal TWO = new BigDecimal("2.0");

	@Test
	void testRangeConstruction() {
		DecimalRange decimalRange = new DecimalRange(bd(1), null, bd(2), null);
		assertThat(decimalRange.lowerBound()).isEqualTo(ONE);
		assertThat(decimalRange.lowerBoundInclusive()).isTrue();
		assertThat(decimalRange.upperBound()).isEqualTo(BigDecimal.valueOf(2, 0));
		assertThat(decimalRange.upperBoundInclusive()).isTrue();

		assertThat(decimalRange).isEqualTo(closedClosed(bd(1), bd(2)));
		assertThat(new DecimalRange(bd(1), null, null, bd(2))).isEqualTo(closedOpen(bd(1), bd(2)));
		assertThat(new DecimalRange(null, bd(1), bd(2), null)).isEqualTo(openClosed(bd(1), bd(2)));
		assertThat(new DecimalRange(null, bd(1), null, bd(2))).isEqualTo(openOpen(bd(1), bd(2)));

		assertThatThrownBy(() -> closedClosed(null, null)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> openClosed(null, null)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> closedOpen(null, null)).isInstanceOf(IllegalArgumentException.class);

		assertThatThrownBy(() -> closedOpen(bd(1), bd(0))).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new DecimalRange(bd(1), null, bd(0), null)).isInstanceOf(IllegalArgumentException.class);

		assertThatThrownBy(() -> closedClosed(bd(1), bd(0))).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> openClosed(bd(1), bd(1))).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> closedOpen(bd(1), bd(1))).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> openOpen(bd(1), bd(1))).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void testRangeInspection() {
		assertThat(new DecimalRange(ZERO, true, ONE_AND_A_HALF, true).isIntegerRange(false)).isFalse();
		assertThat(new DecimalRange(ZERO, true, TWO, true).isIntegerRange(false)).isFalse();
		assertThat(new DecimalRange(ZERO, true, ONE, true).isIntegerRange(false)).isTrue();

		assertThat(new DecimalRange(ZERO, true, ONE_AND_A_HALF, true).isIntegerRange(true)).isFalse();
		assertThat(new DecimalRange(ZERO, true, TWO, true).isIntegerRange(true)).isTrue();
		assertThat(new DecimalRange(ZERO, true, ONE, true).isIntegerRange(true)).isTrue();

		DecimalRange leftOpen = openClosed(null, ONE_AND_A_HALF);
		assertThat(leftOpen.isBounded()).isFalse();
		assertThat(leftOpen.isIntegerRange(false)).isFalse();
		assertThat(leftOpen.isIntegerRange(true)).isFalse();
		assertThat(leftOpen.requiredPrecision()).isEqualTo(2);
		assertThat(leftOpen.requiredScale()).isEqualTo(1);

		DecimalRange rightOpen = closedOpen(ONE_AND_A_HALF, null);
		assertThat(rightOpen.requiredPrecision()).isEqualTo(2);
		assertThat(rightOpen.requiredScale()).isEqualTo(1);

		// Required bits include a sign bit
		DecimalRange range_1_31 = closedOpen(ONE, new BigDecimal("31.00"));
		assertThat(range_1_31.integerBitSize()).isEqualTo(6);
		assertThat(range_1_31.isBounded()).isTrue();
		assertThat(range_1_31.isIntegerRange(false)).isFalse();
		assertThat(range_1_31.isIntegerRange(true)).isTrue();

		DecimalRange range_m1_null = closedOpen(ONE.negate(), null);
		assertThat(range_m1_null.integerBitSize()).isEqualTo(2);
		assertThat(range_m1_null.isBounded()).isFalse();
		assertThat(range_m1_null.isIntegerRange(false)).isTrue();
		assertThat(range_m1_null.isIntegerRange(true)).isTrue();

		assertThat(openClosed(null, ONE).integerBitSize()).isEqualTo(2);
		assertThat(openOpen(null, null).integerBitSize()).isEqualTo(0);
	}

	@Test
	void testRangesAsStrings() {
		assertThat(openOpen(null, null).toString()).isEqualTo("(-inf, inf)");
		assertThat(closedOpen(bd(1), bd(2, 10)).toString()).isEqualTo("[1, 2.10)");
		assertThat(openClosed(null, bd(2, 10)).toString()).isEqualTo("(-inf, 2.10]");
		assertThat(closedClosed(bd(-5, 3), bd(2, 10)).toString()).isEqualTo("[-5.3, 2.10]");
	}

	@Test
	void verifyRangeExtension() {
		// All tests are double: extendsWith should be commutative

		// infinity always wins
		assertThat(openClosed(null, bd(1)).extendWith(closedOpen(bd(2), null))).isEqualTo(openOpen(null, null));
		assertThat(closedOpen(bd(2), null).extendWith(openClosed(null, bd(1)))).isEqualTo(openOpen(null, null));

		// assertThat(closedOpen(bd(1), null).restrictTo(openClosed(null, bd(2)))).isEqualTo(closedClosed(bd(1), bd(2)));
		// assertThat(openClosed(null, bd(2)).restrictTo(closedOpen(bd(1), null))).isEqualTo(closedClosed(bd(1), bd(2)));

		// when equal, closed wins
		assertThat(closedOpen(bd(1), bd(2)).extendWith(openClosed(bd(1), bd(2)))).isEqualTo(closedClosed(bd(1), bd(2)));
		assertThat(openClosed(bd(1), bd(2)).extendWith(closedOpen(bd(1), bd(2)))).isEqualTo(closedClosed(bd(1), bd(2)));

		// when equal, largest scale wins
		assertThat(closedClosed(bd(1, 5), bd(2, 500)).extendWith(closedClosed(bd(1, 500), bd(2, 5)))).isEqualTo(closedClosed(bd(1, 500), bd(2, 500)));
		assertThat(closedClosed(bd(1, 500), bd(2, 5)).extendWith(closedClosed(bd(1, 5), bd(2, 500)))).isEqualTo(closedClosed(bd(1, 500), bd(2, 500)));

		// overlapping ranges work
		assertThat(openClosed(null, bd(1)).extendWith(openClosed(null, bd(2)))).isEqualTo(openClosed(null, bd(2)));
		assertThat(openClosed(null, bd(2)).extendWith(openClosed(null, bd(1)))).isEqualTo(openClosed(null, bd(2)));
		assertThat(closedOpen(bd(1), null).extendWith(closedOpen(bd(2), null))).isEqualTo(closedOpen(bd(1), null));
		assertThat(closedOpen(bd(2), null).extendWith(closedOpen(bd(1), null))).isEqualTo(closedOpen(bd(1), null));

		// non-overlapping ranges also work
		assertThat(openClosed(bd(1), bd(2)).extendWith(closedOpen(bd(3), bd(4)))).isEqualTo(openOpen(bd(1), bd(4)));
		assertThat(closedOpen(bd(3), bd(4)).extendWith(openClosed(bd(1), bd(2)))).isEqualTo(openOpen(bd(1), bd(4)));
	}

	@Test
	void verifyRangeRestriction() {
		// All tests are double: extendsWith should be commutative

		// infinity never wins
		assertThat(closedOpen(bd(1), null).restrictTo(openClosed(null, bd(2)))).isEqualTo(closedClosed(bd(1), bd(2)));
		assertThat(openClosed(null, bd(2)).restrictTo(closedOpen(bd(1), null))).isEqualTo(closedClosed(bd(1), bd(2)));

		// when equal, open wins
		assertThat(closedOpen(bd(1), bd(2)).restrictTo(openClosed(bd(1), bd(2)))).isEqualTo(openOpen(bd(1), bd(2)));
		assertThat(openClosed(bd(1), bd(2)).restrictTo(closedOpen(bd(1), bd(2)))).isEqualTo(openOpen(bd(1), bd(2)));

		// when equal, largest scale wins
		assertThat(closedClosed(bd(1, 5), bd(2, 500)).restrictTo(closedClosed(bd(1, 500), bd(2, 5)))).isEqualTo(closedClosed(bd(1, 500), bd(2, 500)));
		assertThat(closedClosed(bd(1, 500), bd(2, 5)).restrictTo(closedClosed(bd(1, 5), bd(2, 500)))).isEqualTo(closedClosed(bd(1, 500), bd(2, 500)));

		// overlapping ranges work
		assertThat(openClosed(null, bd(1)).restrictTo(openClosed(null, bd(2)))).isEqualTo(openClosed(null, bd(1)));
		assertThat(openClosed(null, bd(2)).restrictTo(openClosed(null, bd(1)))).isEqualTo(openClosed(null, bd(1)));
		assertThat(closedOpen(bd(1), null).restrictTo(closedOpen(bd(2), null))).isEqualTo(closedOpen(bd(2), null));
		assertThat(closedOpen(bd(2), null).restrictTo(closedOpen(bd(1), null))).isEqualTo(closedOpen(bd(2), null));

		// non-overlapping ranges cannot work
		assertThatThrownBy(() -> openClosed(bd(1), bd(2)).restrictTo(closedOpen(bd(3), bd(4)))).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> closedOpen(bd(3), bd(4)).restrictTo(openClosed(bd(1), bd(2)))).isInstanceOf(IllegalArgumentException.class);
	}

	private DecimalRange openClosed(BigDecimal left, BigDecimal right) {
		return new DecimalRange(left, false, right, true);
	}

	private DecimalRange closedOpen(BigDecimal left, BigDecimal right) {
		return new DecimalRange(left, true, right, false);
	}

	private DecimalRange openOpen(BigDecimal left, BigDecimal right) {
		return new DecimalRange(left, false, right, false);
	}

	private DecimalRange closedClosed(BigDecimal left, BigDecimal right) {
		return new DecimalRange(left, true, right, true);
	}

	private static BigDecimal bd(int number) {
		return new BigDecimal(number);
	}

	private static BigDecimal bd(int number, int fraction) {
		return new BigDecimal(number + "." + fraction);
	}
}
