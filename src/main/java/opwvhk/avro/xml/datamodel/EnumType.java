package opwvhk.avro.xml.datamodel;

import java.util.List;

public record EnumType(String name, String documentation, List<String> enumSymbols, String defaultSymbol) implements ScalarType {
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
	public String debugString(String indent) {
		String symbols = String.join(", ", enumSymbols);
		String symbolsWithDefault = defaultSymbol == null ? symbols : symbols + "; " + defaultSymbol;
		return indent + "enum(" + name + ": " + symbolsWithDefault + ")";
	}
}
