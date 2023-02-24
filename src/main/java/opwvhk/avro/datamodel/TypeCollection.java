package opwvhk.avro.datamodel;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collection of {@link StructType StructTypes}. Used to make types more easily accessible, and to enforce uniqueness of names.
 */
public class TypeCollection {
	private final Map<String, NamedElement> typesByName;
	private final Set<NamedElement> types;

	public TypeCollection() {
		typesByName = new HashMap<>();
		types = new LinkedHashSet<>();
	}

	public NamedElement getType(String name) {
		return typesByName.get(name);
	}

	void addType(NamedElement type) {
		List<String> allNames = type.nameAndAliases();
		Set<String> duplicates = new LinkedHashSet<>(allNames);
		duplicates.retainAll(typesByName.keySet());
		if (!duplicates.isEmpty())
			throw new IllegalArgumentException("Type already exists: it has name&aliases %s, but these names are already used: %s."
					.formatted(String.join(", ", allNames), String.join(", ", duplicates)));
		addNewAliases(type);
		types.add(type);
	}

	void addNewAliases(NamedElement type) {
		type.nameAndAliases().forEach(name -> typesByName.put(name, type));
	}
}
