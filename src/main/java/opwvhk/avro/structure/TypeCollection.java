package opwvhk.avro.structure;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableCollection;

/**
 * Collection of {@link StructType StructTypes}. Used to make types more easily accessible, and to enforce uniqueness of names.
 */
public class TypeCollection implements Iterable<StructType> {
	private final Map<String, StructType> types;

	public TypeCollection() {
		this.types = new LinkedHashMap<>();
	}

	public StructType getType(String name) {
		StructType structType = types.get(name);
		if (structType != null) {
			return structType;
		}
		for (StructType type : types.values()) {
			if (name.equals(type.name()) || type.aliases().contains(name)) {
				return type;
			}
		}
		return null;
	}

	public void addType(StructType type) {
		List<String> allNames = type.nameAndAliases();
		for (StructType existing : types.values()) {
			List<String> existingNames = existing.nameAndAliases();
			if (!existing.equals(type) && existingNames.stream().anyMatch(allNames::contains)) {
				throw new IllegalArgumentException("Type already exists: it has name&aliases %s, but there is already a type with name&aliases %s."
						.formatted(allNames, existingNames));
			}
		}
		types.put(type.name(), type);
	}

	@Override
	public Iterator<StructType> iterator() {
		return unmodifiableCollection(types.values()).iterator();
	}
}
