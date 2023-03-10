package opwvhk.avro.xml.datamodel;

import java.util.List;

import org.apache.avro.Schema;

/**
 * An enumeration.
 *
 * @param name          the (full) name fo the enumeration
 * @param documentation any documentation describing the enumeration
 * @param enumSymbols   the allowed symbols
 * @param defaultSymbol the symbol to use if an invalid symbol is parsed
 */
public record EnumType(String name, String documentation, List<String> enumSymbols, String defaultSymbol) implements ScalarType {
	/**
	 * Create an {@code EnumType}.
	 */
	public EnumType {
		if (defaultSymbol != null && !enumSymbols.contains(defaultSymbol)) {
			throw new IllegalArgumentException(
					"%s is not a valid symbol; expected one of: %s".formatted(defaultSymbol, String.join(", ", enumSymbols)));
		}
	}

	@Override
	public Object parseNonNull(String text) {
		if (enumSymbols.contains(text)) {
			return text;
		} else if (defaultSymbol != null) {
			return defaultSymbol;
		}
		throw new IllegalArgumentException("Unknown enum symbol for %s: %s".formatted(name(), text));
	}

	@Override
	public Schema toSchema() {
		return Schema.createEnum(name, documentation, null, enumSymbols);
	}

	@Override
	public String debugString(String indent) {
		String symbols = String.join(", ", enumSymbols);
		String symbolsWithDefault = defaultSymbol == null ? symbols : symbols + "; " + defaultSymbol;
		return indent + "enum(" + name + ": " + symbolsWithDefault + ")";
	}
}
