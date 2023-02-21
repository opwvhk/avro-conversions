package opwvhk.avro.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface NamedElement {
	/**
	 * Combine a name and aliases into a single list with distinct elements.
	 *
	 * @param name a name
	 * @param aliases zero or more aliases
	 * @return a list with all names and aliases
	 * @throws IllegalArgumentException if there are duplicate name/aliases
	 */
	static List<String> names(String name, Collection<String> aliases) {
		List<String> names = new ArrayList<>(List.of(name));
		List<String> duplicates = new ArrayList<>();
		addDistinct(names, duplicates, aliases);
		if (!duplicates.isEmpty()) {
			throw new IllegalArgumentException("Duplicate name/aliases: " + String.join(", ", duplicates));
		}
		return names;
	}

	/**
	 * Add distinct elements to a collection. All new elements that the collection does not already contain are added.
	 *
	 * @param collection a collection to add distinct elements to
	 * @param duplicates a collection to add duplicate elements to
	 * @param newElements the elements to add
	 */
	static <T> void addDistinct(Collection<T> collection, Collection<T> duplicates, Iterable<T> newElements) {
		for (T newElement : newElements) {
			boolean isDuplicate = collection.contains(newElement);
			Collection<T> col = isDuplicate ? duplicates : collection;
			col.add(newElement);
		}
	}

	List<String> nameAndAliases();

	default String name() {
		return nameAndAliases().get(0);
	}

	default List<String> aliases() {
		return nameAndAliases().subList(1, nameAndAliases().size());
	}
}
