package opwvhk.avro.structure;

import java.util.Collection;
import java.util.List;

public record EnumType(List<String> nameAndAliases, String documentation, List<String> enumSymbols, String defaultSymbol) implements ScalarType, NamedElement {
	public EnumType(String name, Collection<String> aliases, String documentation, List<String> enumSymbols, String defaultSymbol) {
		this(NamedElement.names(name, aliases), documentation, enumSymbols, defaultSymbol);
	}

	public EnumType(String name, String documentation, List<String> enumSymbols) {
		this(List.of(name), documentation, enumSymbols, null);
	}

	public EnumType withDefaultSymbol(String symbol) {
		if (!enumSymbols.contains(symbol)) {
			throw new IllegalArgumentException(
					"%s is not a valid symbol; expected one of: %s".formatted(symbol, String.join(", ", enumSymbols)));
		}
		return new EnumType(nameAndAliases, documentation, enumSymbols, symbol);
	}

	@Override
	public Object parseNonNull(String text) {
		return text;
	}

	@Override
	public String debugString(String indent) {
		return indent + "enum(%s)".formatted(String.join(", ", enumSymbols));
	}
}
