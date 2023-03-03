package opwvhk.avro.xml.datamodel;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Collection of {@link StructType StructTypes}. Used to make types more easily accessible, and to enforce uniqueness of names.
 */
public class TypeCollection {
	private final Map<String, Type> typesByName;
	private final Set<Type> types;

	public TypeCollection() {
		typesByName = new HashMap<>();
		types = new LinkedHashSet<>();
	}

	public Type getType(String name) {
		return typesByName.get(name);
	}

	void addType(StructType type) {
		addType(type, type.name(), type.aliases());
	}

	void addType(EnumType type) {
		addType(type, type.name(), type.aliases());
	}

	private void addType(Type type, String name, Set<String> aliases) {
		Set<String> duplicates = new LinkedHashSet<>();
		duplicates.add(name);
		duplicates.addAll(aliases);
		duplicates.retainAll(typesByName.keySet());
		if (!duplicates.isEmpty()) {
			throw new IllegalArgumentException("Type already exists: these names/aliases are already used: %s.".formatted(String.join(", ", duplicates)));
		}
		types.add(type);
		typesByName.put(name, type);
		aliases.forEach(alias -> typesByName.put(alias, type));
	}

	void addName(String name, Type type) {
		typesByName.put(name, requireNonNull(types.stream().filter(type::equals).findAny().orElse(null)));
	}
}
