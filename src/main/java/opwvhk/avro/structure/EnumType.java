package opwvhk.avro.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import opwvhk.avro.util.Utils;

public final class EnumType implements ScalarType, NamedElement {
	private TypeCollection typeCollection;
	private final List<String> nameAndAliases;
	private String documentation;
	private final List<String> enumSymbols;
	private final String defaultSymbol;

	EnumType(TypeCollection typeCollection, List<String> nameAndAliases, String documentation, List<String> enumSymbols, String defaultSymbol) {
		if (defaultSymbol != null && !enumSymbols.contains(defaultSymbol)) {
			throw new IllegalArgumentException(
					"%s is not a valid symbol; expected one of: %s".formatted(defaultSymbol, String.join(", ", enumSymbols)));
		}
		this.typeCollection = typeCollection;
		this.nameAndAliases = new ArrayList<>(nameAndAliases);
		this.documentation = documentation;
		this.enumSymbols = enumSymbols;
		this.defaultSymbol = defaultSymbol;

		if (typeCollection != null) {
			typeCollection.addType(this);
		}
	}

	public EnumType(TypeCollection typeCollection, String name, String documentation, List<String> enumSymbols, String defaultSymbol) {
		this(typeCollection, List.of(name), documentation, enumSymbols, defaultSymbol);
	}

	EnumType(TypeCollection typeCollection, String name, Collection<String> aliases, String documentation, List<String> enumSymbols, String defaultSymbol) {
		this(typeCollection, NamedElement.names(name, aliases), documentation, enumSymbols, defaultSymbol);
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

	void setTypeCollection(TypeCollection typeCollection) {
		if (typeCollection != this.typeCollection) {
			this.typeCollection = typeCollection;
			typeCollection.addType(this);
		}
	}

	@Override
	public List<String> nameAndAliases() {
		return nameAndAliases;
	}

	public void rename(String newName) {
		NamedElement existingType = typeCollection.getType(newName);
		if (existingType != null) {
			throw new IllegalArgumentException("There is already a type with name/alias %s: is has these names: %s".formatted(newName,
					String.join(", ", existingType.nameAndAliases())));
		}
		nameAndAliases.add(0, newName);
	}

	public String documentation() {
		return documentation;
	}

	public void setDocumentation(String newDocumentation) {
		documentation = newDocumentation;
	}

	public List<String> enumSymbols() {
		return enumSymbols;
	}

	public String defaultSymbol() {
		return defaultSymbol;
	}

	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	@Override
	public boolean equals(Object obj) {
		return Utils.recursionSafeEquals(this, obj, EnumType::nameAndAliases, EnumType::documentation, EnumType::enumSymbols, EnumType::defaultSymbol);
	}

	@Override
	public int hashCode() {
		return Utils.recursionSafeHashCode(this, nameAndAliases, documentation, enumSymbols, defaultSymbol);
	}

	@Override
	public String debugString(String indent) {
		String names = String.join(", ", nameAndAliases);
		String symbols = String.join(", ", enumSymbols);
		String string = defaultSymbol == null ? "enum(%s: %s)".formatted(names, symbols) : "enum(%s: %s; %s)".formatted(names, symbols, defaultSymbol);
		return indent + string;
	}
}
