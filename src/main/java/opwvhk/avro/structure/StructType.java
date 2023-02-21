package opwvhk.avro.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

public final class StructType implements Type, NamedElement {
	private final TypeCollection typeCollection;
	private final List<String> nameAndAliases;
	private final String documentation;
	private List<Field> fields;
	private Map<String, Field> fieldsByName;

	public StructType(TypeCollection typeCollection, String name, String documentation) {
		this.typeCollection = typeCollection;
		this.documentation = documentation;

		nameAndAliases = new ArrayList<>();
		nameAndAliases.add(name);

		fields = null;
		fieldsByName = null;

		typeCollection.addType(this);
	}

	// For testing, create copy with sorted fields.
	private StructType(StructType original) {
		this.typeCollection = null;
		this.nameAndAliases = original.nameAndAliases();
		this.documentation = original.documentation();
		List<Field> fields = new ArrayList<>(this.fields);
		fields.sort(Comparator.comparing(NamedElement::name));
		this.fields = fields;
	}

	public List<String> nameAndAliases() {
		return unmodifiableList(nameAndAliases);
	}

	public void addAlias(String alias) {
		StructType existingType = typeCollection.getType(alias);
		if (existingType != null) {
			throw new IllegalArgumentException("There is already a type with name/alias %s: %s".formatted(alias, existingType));
		}
		nameAndAliases.add(alias);
	}

	public void addAliases(Iterable<String> aliases) {
		aliases.forEach(this::addAlias);
	}

	public String documentation() {
		return documentation;
	}

	public List<Field> fields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		if (this.fields != null) {
			throw new IllegalStateException("Fields can be set only once");
		}
		Set<String> allNames = new HashSet<>();
		Set<String> duplicateNames = new LinkedHashSet<>();
		for (Field field : fields) {
			NamedElement.addDistinct(allNames, duplicateNames, field.nameAndAliases());
		}
		if (!duplicateNames.isEmpty()) {
			throw new IllegalArgumentException("All field names/aliases must be unique. These are not: " + String.join(", ", duplicateNames));
		}
		this.fields = fields;
		fieldsByName = new HashMap<>();
		fields.forEach(f -> f.nameAndAliases().forEach(name -> fieldsByName.put(name, f)));
	}

	public Field getField(String name) {
		return fieldsByName.get(name);
	}

	public StructType sortFieldsForTesting() {
		return new StructType(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StructType that = (StructType) o;

		Seen here = new Seen(this, that);
		Set<Seen> seen = SEEN_EQUALS.get();
		boolean first = seen.isEmpty();
		if (!seen.add(here)) {
			// If not equal, another field will fail the equality check.
			return true;
		}

		boolean r = containSame(nameAndAliases, that.nameAndAliases) && Objects.equals(documentation, that.documentation) && containSame(fields, that.fields);
		if (first) {
			seen.clear();
		}
		return r;
	}
	private static final ThreadLocal<Set<Seen>> SEEN_EQUALS = ThreadLocal.withInitial(HashSet::new);

	private <T> boolean containSame(List<T> list1, List<T> list2) {
		int found = 0;
		for (T t : list2) {
			if (!list1.contains(t)) {
				return false;
			}
			found++;
		}
		return found == list2.size();
	}

	@Override
	public int hashCode() {
		return Objects.hash(nameAndAliases, documentation);
	}

	@Override
	public String toString() {
		return debugString("");
	}

	@Override
	public String debugString(String indent) {
		StringBuilder buffer = new StringBuilder();
		buffer.append(indent).append("StructType(").append(name());
		aliases().forEach(alias -> buffer.append(", ").append(alias));
		buffer.append(")");

		Seen here = new Seen(this);
		Set<Seen> seen = SEEN_DEBUG_STRING.get();
		boolean first = seen.isEmpty();
		if (seen.add(here)) {
			// No infinite recursion yet
			buffer.append(" {");
			if (documentation != null) {
				buffer.append("\n  ").append(indent).append("Doc: ").append(documentation);
			}
			if (fields == null) {
				buffer.append("\n  ").append(indent).append("(no fields yet)");
			} else {
				List<Field> sortedFields = new ArrayList<>(fields);
				sortedFields.sort(Comparator.comparing(NamedElement::name));
				for (Field field : sortedFields) {
					buffer.append("\n  ").append(indent).append(field.name());
					buffer.append(switch (field.cardinality) {
						case MULTIPLE -> "[]";
						case OPTIONAL -> "?";
						default -> "";
					});
					if (field.nameAndAliases().size() > 1) {
						field.aliases().forEach(alias -> buffer.append(", ").append(alias));
					}
					if (field.documentation() != null) {
						buffer.append("\n    ").append(indent).append("Doc: ").append(field.documentation());
					}
					if (field.defaultValue() != null) {
						buffer.append("\n    ").append(indent).append("Default: ").append(field.defaultValue());
					}
					buffer.append("\n").append(field.type().debugString(indent + "    "));
				}
			}
			buffer.append("\n").append(indent).append("}");
		}
		if (first) {
			seen.clear();
		}
		return buffer.toString();
	}
	private static final ThreadLocal<Set<Seen>> SEEN_DEBUG_STRING = ThreadLocal.withInitial(HashSet::new);

	public record Field(List<String> nameAndAliases, String documentation, Cardinality cardinality, Type type, Object defaultValue) implements NamedElement {
		public Field {
			if (type == null) {
				throw new NullPointerException("Cannot create field without a type.");
			}
		}

		public Field(String name, Collection<String> aliases, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
			this(NamedElement.names(name, aliases), documentation, cardinality, type, defaultValue);
		}

		public Field(String name, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
			this(List.of(name), documentation, cardinality, type, defaultValue);
		}
	}

	/**
	 * Simple class with equality check (!) on its contents, used to prevent infinite recursion.
	 */
	private static class Seen {
		private final Object left;
		private final Object right;

		public Seen(Object left, Object right) {
			this.left = left;
			this.right = right;
		}

		public Seen(Object obj) {
			this(obj, null);
		}

		public boolean equals(Object o) {
			if (!(o instanceof Seen))
				return false;
			return this.left == ((Seen) o).left && this.right == ((Seen) o).right;
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(left) + System.identityHashCode(right);
		}
	}
}
