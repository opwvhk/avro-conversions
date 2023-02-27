package opwvhk.avro.datamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import opwvhk.avro.util.Utils;

public final class EnumType implements ScalarType {
	private TypeCollection typeCollection;
	private String name;
	private final Set<String> aliases;
	private String documentation;
	private final List<String> enumSymbols;
	private final String defaultSymbol;

	EnumType(TypeCollection typeCollection, String name, Set<String> aliases, String documentation, List<String> enumSymbols, String defaultSymbol) {
		if (aliases.contains(name)) {
			throw new IllegalArgumentException("Duplicate name/aliases: " + String.join(", ", name));
		}
		if (defaultSymbol != null && !enumSymbols.contains(defaultSymbol)) {
			throw new IllegalArgumentException(
					"%s is not a valid symbol; expected one of: %s".formatted(defaultSymbol, String.join(", ", enumSymbols)));
		}
		this.typeCollection = typeCollection;
		this.name = name;
		this.aliases = new HashSet<>(aliases);
		this.documentation = documentation;
		this.enumSymbols = enumSymbols;
		this.defaultSymbol = defaultSymbol;

		if (typeCollection != null) {
			typeCollection.addType(this);
		}
	}

	public EnumType(TypeCollection typeCollection, String name, String documentation, List<String> enumSymbols, String defaultSymbol) {
		this(typeCollection, name, Set.of(), documentation, enumSymbols, defaultSymbol);
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

	public String name() {
		return name;
	}

	public Set<String> aliases() {
		return aliases;
	}

	public void rename(String newName) {
		Type existingType = typeCollection.getType(newName);
		if (existingType != null) {
			throw new IllegalArgumentException("There is already a type with that name/alias: %s".formatted(existingType));
		}
		aliases.remove(newName);
		aliases.add(name);
		name = newName;
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
		return Utils.recursionSafeEquals(this, obj, EnumType::name, EnumType::aliases, EnumType::documentation, EnumType::enumSymbols, EnumType::defaultSymbol);
	}

	@Override
	public int hashCode() {
		return Utils.recursionSafeHashCode(this, name, aliases, documentation, enumSymbols, defaultSymbol);
	}

	@Override
	public String debugString(String indent) {
		String names = aliases.isEmpty() ? name : name + ", " + String.join(", ", aliases);
		String symbols = String.join(", ", enumSymbols);
		String symbolsWithDefault = defaultSymbol == null ? symbols : symbols + "; " + defaultSymbol;
		return indent + "enum(" + names + ": " + symbolsWithDefault + ")";
	}
}
