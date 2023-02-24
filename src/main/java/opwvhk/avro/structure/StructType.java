package opwvhk.avro.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import opwvhk.avro.util.Utils;

import static java.util.Arrays.asList;
import static opwvhk.avro.util.Utils.nonRecursive;

public final class StructType implements Type, NamedElement {
	private final TypeCollection typeCollection;
	private final List<String> nameAndAliases;
	private String documentation;
	private List<Field> fields;
	private Map<String, Field> fieldsByName;

	public StructType(TypeCollection typeCollection, String name, String documentation) {
		this(typeCollection, name, List.of(), documentation);
	}

	public StructType(TypeCollection typeCollection, String name, Collection<String> aliases, String documentation) {
		this.typeCollection = typeCollection;
		this.documentation = documentation;

		nameAndAliases = NamedElement.names(name, aliases);

		fields = null;
		fieldsByName = null;

		typeCollection.addType(this);
	}

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
		typeCollection.addNewAliases(this);
	}

	public String documentation() {
		return documentation;
	}

	public void setDocumentation(String documentation) {
		this.documentation = documentation;
	}

	public List<Field> fields() {
		return fields;
	}

	public StructType withFields(Field... fields) {
		setFields(asList(fields));
		return this;
	}

	public void setFields(List<Field> fields) {
		if (this.fields != null) {
			throw new IllegalStateException("Fields can be set only once");
		}
		Set<String> duplicateNames = new LinkedHashSet<>();
		HashMap<String, Field> fieldsByName = new HashMap<>();
		for (Field field : fields) {
			for (String name : field.nameAndAliases()) {
				if (fieldsByName.putIfAbsent(name, field) != null) {
					duplicateNames.add(name);
				}
			}
		}
		if (!duplicateNames.isEmpty()) {
			throw new IllegalArgumentException("All field names/aliases must be unique. These are not: " + String.join(", ", duplicateNames));
		}
		for (Field field : fields) {
			field.setStructType(this);
			if (field.type() instanceof EnumType enumType) {
				enumType.setTypeCollection(typeCollection);
			}
		}
		this.fieldsByName = fieldsByName;
		this.fields = List.copyOf(fields);
	}

	public Field getField(String name) {
		return fieldsByName.get(name);
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
		return nonRecursive("StructEquality", this, that, true,
				() -> nameAndAliases.equals(that.nameAndAliases) && Objects.equals(documentation, that.documentation) &&
				      containSame(fields, that.fields));
	}

	private <T> boolean containSame(List<T> list1, List<T> list2) {
		int found = 0;
		for (T t : list2) {
			if (!list1.contains(t)) {
				return false;
			}
			found++;
		}
		return found == list1.size();
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
		return indent + "StructType(" + String.join(", ", nameAndAliases()) + ")" +
		       nonRecursive("debugString", this, "", () -> {
			       StringBuilder buf = new StringBuilder();
			       // No infinite recursion yet
			       buf.append(" {");
			       if (documentation != null) {
				       buf.append("\n  ").append(indent).append("Doc: ").append(documentation);
			       }
			       if (fields == null) {
				       buf.append("\n  ").append(indent).append("(no fields yet)");
			       } else {
				       List<Field> sortedFields = new ArrayList<>(fields);
				       sortedFields.sort(Comparator.comparing(NamedElement::name));
				       for (Field field : sortedFields) {
					       buf.append("\n  ").append(indent).append(field.name());
					       buf.append(switch (field.cardinality) {
						       case MULTIPLE -> "[]";
						       case OPTIONAL -> "?";
						       default -> "";
					       });
					       if (field.nameAndAliases().size() > 1) {
						       field.aliases().forEach(alias -> buf.append(", ").append(alias));
					       }
					       if (field.documentation() != null) {
						       buf.append("\n    ").append(indent).append("Doc: ").append(field.documentation());
					       }
					       if (field.defaultValue() != null) {
						       buf.append("\n    ").append(indent).append("Default: ").append(field.defaultValue());
					       }
					       buf.append("\n").append(field.type().debugString(indent + "    "));
				       }
			       }
			       buf.append("\n").append(indent).append("}");
			       return buf;
		       });
	}

	public static final class Field implements NamedElement {
		public static final Object NULL_VALUE = new Object();
		private StructType structType;
		private final List<String> nameAndAliases;
		private String documentation;
		private final Cardinality cardinality;
		private final Type type;
		private final Object defaultValue;

		public Field(List<String> nameAndAliases, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
			if (type == null) {
				throw new NullPointerException("Cannot create field without a type.");
			}
			structType = null;
			this.nameAndAliases = nameAndAliases;
			this.documentation = documentation;
			this.cardinality = cardinality;
			this.type = type;
			this.defaultValue = defaultValue;
		}

		public Field(String name, Collection<String> aliases, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
			this(NamedElement.names(name, aliases), documentation, cardinality, type, defaultValue);
		}

		public Field(String name, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
			this(new ArrayList<>(List.of(name)), documentation, cardinality, type, defaultValue);
		}

		void setStructType(StructType structType) {
			if (this.structType != null) {
				throw new IllegalStateException("Field %s is already part of a StructType".formatted(name()));
			}
			this.structType = structType;
		}

		@Override
		public List<String> nameAndAliases() {
			return nameAndAliases;
		}

		public void rename(String newName) {
			if (structType != null) {
				NamedElement existingField = structType.getField(newName);
				if (existingField != null) {
					throw new IllegalArgumentException("There is already a field with name/alias %s: is has these names: %s".formatted(newName,
							String.join(", ", existingField.nameAndAliases())));
				}
				structType.fieldsByName.put(newName, this);
			} else if (nameAndAliases.contains(newName)) {
				throw new IllegalArgumentException("This field already has that name.");
			}
			nameAndAliases.add(0, newName);
		}

		public String documentation() {
			return documentation;
		}

		public void setDocumentation(String newDocumentation) {
			documentation = newDocumentation;
		}

		public Cardinality cardinality() {
			return cardinality;
		}

		public Type type() {
			return type;
		}

		public Object defaultValue() {
			return defaultValue;
		}

		@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
		@Override
		public boolean equals(Object o) {
			return Utils.recursionSafeEquals(this, o, Field::nameAndAliases, Field::documentation, Field::cardinality, Field::type, Field::defaultValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(nameAndAliases, documentation, cardinality, type, defaultValue);
		}
	}
}
