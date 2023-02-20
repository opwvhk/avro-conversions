package opwvhk.avro.xsd;

import javax.xml.namespace.QName;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.apache.ws.commons.schema.walker.XmlSchemaRestriction;
import org.apache.ws.commons.schema.walker.XmlSchemaTypeInfo;

import static java.math.BigInteger.ONE;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Collections.emptyList;
import static org.apache.ws.commons.schema.constants.Constants.XSD_ANYURI;
import static org.apache.ws.commons.schema.constants.Constants.XSD_BASE64;
import static org.apache.ws.commons.schema.constants.Constants.XSD_BOOLEAN;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DATE;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DATETIME;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DECIMAL;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DOUBLE;
import static org.apache.ws.commons.schema.constants.Constants.XSD_FLOAT;
import static org.apache.ws.commons.schema.constants.Constants.XSD_HEXBIN;
import static org.apache.ws.commons.schema.constants.Constants.XSD_INT;
import static org.apache.ws.commons.schema.constants.Constants.XSD_LONG;
import static org.apache.ws.commons.schema.constants.Constants.XSD_STRING;
import static org.apache.ws.commons.schema.constants.Constants.XSD_TIME;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.DIGITS_FRACTION;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.DIGITS_TOTAL;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.ENUMERATION;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.EXCLUSIVE_MAX;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.EXCLUSIVE_MIN;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.INCLUSIVE_MAX;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.INCLUSIVE_MIN;

public sealed interface ScalarType
		permits FixedType, DecimalType, EnumType {
	static Set<QName> userRecognizedTypes() {
		return Set.of(XSD_BOOLEAN, XSD_FLOAT, XSD_DOUBLE, XSD_DATE, XSD_DATETIME, XSD_TIME, XSD_INT, XSD_LONG, XSD_DECIMAL, XSD_STRING, XSD_ANYURI,
				XSD_HEXBIN, XSD_BASE64);
	}

	static ScalarType fromTypeInfo(XmlSchemaTypeInfo typeInfo) {
		QName recognizedType = typeInfo.getUserRecognizedType();
		if (XSD_BOOLEAN.equals(recognizedType)) {
			return FixedType.BOOLEAN;
		} else if (XSD_FLOAT.equals(recognizedType)) {
			return FixedType.FLOAT;
		} else if (XSD_DOUBLE.equals(recognizedType)) {
			return FixedType.DOUBLE;
		} else if (XSD_DATE.equals(recognizedType)) {
			return FixedType.DATE;
		} else if (XSD_DATETIME.equals(recognizedType)) {
			return FixedType.DATETIME;
		} else if (XSD_TIME.equals(recognizedType)) {
			return FixedType.TIME;
		} else if (XSD_INT.equals(recognizedType)) {
			// XSD_BYTE and XSD_SHORT match this branch unless overridden.
			return DecimalType.INTEGER_TYPE;
		} else if (XSD_LONG.equals(recognizedType)) {
			return DecimalType.LONG_TYPE;
		} else if (XSD_DECIMAL.equals(recognizedType)) {
			// XSD_INT and XSD_LONG would match this branch, but they're overridden.
			final int fractionDigits = restriction(typeInfo, DIGITS_FRACTION, Integer::valueOf).orElseThrow(
					() -> new IllegalArgumentException("xs:decimal without precision"));
			Optional<Integer> totalDigits = restriction(typeInfo, DIGITS_TOTAL, Integer::valueOf);
			UnaryOperator<BigDecimal> round = bd -> bd.setScale(fractionDigits, HALF_UP);
			UnaryOperator<BigDecimal> incULP = bd -> new BigDecimal(bd.unscaledValue().add(ONE), fractionDigits);
			UnaryOperator<BigDecimal> decULP = bd -> new BigDecimal(bd.unscaledValue().subtract(ONE), fractionDigits);
			Optional<BigDecimal> minInclusive = restriction(typeInfo, INCLUSIVE_MIN, BigDecimal::new).map(round);
			Optional<BigDecimal> minExclusive = restriction(typeInfo, EXCLUSIVE_MIN, BigDecimal::new).map(round).map(incULP);
			Optional<BigDecimal> maxInclusive = restriction(typeInfo, INCLUSIVE_MAX, BigDecimal::new).map(round);
			Optional<BigDecimal> maxExclusive = restriction(typeInfo, EXCLUSIVE_MAX, BigDecimal::new).map(round).map(decULP);
			Optional<BigDecimal> maxDigits = totalDigits.map(BigDecimal.TEN::pow).map(decULP);
			Optional<BigDecimal> minDigits = maxDigits.map(BigDecimal::negate);

			int numberOfDigits = Stream.concat(totalDigits.stream(), Stream.of(minInclusive, minExclusive, maxInclusive, maxExclusive)
					.flatMap(Optional::stream).map(BigDecimal::precision)).max(Integer::compareTo).orElse(Integer.MAX_VALUE);
			if (fractionDigits > 0) {
				return DecimalType.withFraction(numberOfDigits, fractionDigits);
			} else {
				// Calculate number of bits; defaulting to Long.SIZE (this coerces unconstrained integers to a Long)
				int numberOfBits = Stream.of(minInclusive, minExclusive, maxInclusive, maxExclusive, maxDigits, minDigits).flatMap(Optional::stream).map(
								BigDecimal::unscaledValue).map(BigInteger::bitLength).max(Integer::compareTo).map(b -> b + 1) // Plus sign bit
						.orElse(Long.SIZE);
				return DecimalType.integer(numberOfBits, numberOfDigits);
			}
		} else if (XSD_STRING.equals(recognizedType)) {
			List<String> symbols = restriction(typeInfo, ENUMERATION).toList();
			if (symbols.isEmpty()) {
				return FixedType.STRING;
			} else {
				return new EnumType(symbols);
			}
		} else if (XSD_ANYURI.equals(recognizedType)) {
			return FixedType.STRING;
		} else if (XSD_HEXBIN.equals(recognizedType)) {
			return FixedType.BINARY_HEX;
		} else if (XSD_BASE64.equals(recognizedType)) {
			return FixedType.BINARY_BASE64;
		} else {
			throw new IllegalArgumentException("Unsupported simple type: " + typeInfo);
		}
	}

	private static <T> Optional<T> restriction(XmlSchemaTypeInfo typeInfo, XmlSchemaRestriction.Type restrictionType, Function<String, T> converter) {
		return restriction(typeInfo, restrictionType).map(converter).findFirst();
	}

	private static Stream<String> restriction(XmlSchemaTypeInfo typeInfo, XmlSchemaRestriction.Type restrictionType) {
		return Stream.ofNullable(typeInfo.getFacets()).map(m -> m.getOrDefault(restrictionType, emptyList())).flatMap(List::stream).map(
				XmlSchemaRestriction::getValue).map(Object::toString);
	}

	String toString();
}
