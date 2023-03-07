package opwvhk.avro.xml.datamodel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import opwvhk.avro.util.Utils;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static opwvhk.avro.util.Utils.nonRecursive;

public final class StructType implements Type {
	private final String name;
	private final String documentation;
	private List<Field> fields;
	private Map<String, Field> fieldsByName;

	public StructType(String name, String documentation) {
		this.name = name;
		this.documentation = documentation;
		fields = null;
		fieldsByName = null;
	}

	public String name() {
		return name;
	}

	public String documentation() {
		return documentation;
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
			String name = field.name();
			if (fieldsByName.putIfAbsent(name, field) != null) {
				duplicateNames.add(name);
			}
		}
		if (!duplicateNames.isEmpty()) {
			throw new IllegalArgumentException("All field names must be unique. These are not: " + String.join(", ", duplicateNames));
		}
		for (Field field : fields) {
			field.setStructType(this);
		}
		this.fieldsByName = fieldsByName;
		this.fields = List.copyOf(fields);
	}

	Field getField(String name) {
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
				() -> name.equals(that.name) && Objects.equals(documentation, that.documentation) && containSame(fields, that.fields));
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
		return Objects.hash(name, documentation);
	}

	@Override
	public String toString() {
		return debugString("");
	}

	@Override
	public String debugString(String indent) {
		return indent + "StructType(" + name + ")" +
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
				       sortedFields.sort(Comparator.comparing(Field::name));
				       for (Field field : sortedFields) {
					       buf.append("\n  ").append(indent).append(field.cardinality.formatName(field.name));
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

	public static final class Field {
		public static final Object NULL_VALUE = new Object();
		private StructType structType;
		private final String name;
		private final String documentation;
		private final Cardinality cardinality;
		private final Type type;
		private final Object defaultValue;

		public Field(String name, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
			if (type == null) {
				throw new NullPointerException("Cannot create field without a type.");
			}

			structType = null;
			this.name = name;
			this.documentation = documentation;
			this.cardinality = cardinality;
			this.type = type;
			this.defaultValue = switch (cardinality) {
				// Override default value for arrays: nulls cause problems, amd anything else doesn't make sense.
				case MULTIPLE -> emptyList();
				// Optional fields must have a default, as 'optional' applies both to the presence of a value and to whether they must be set.
				case OPTIONAL -> Optional.ofNullable(defaultValue).orElse(StructType.Field.NULL_VALUE);
				default -> defaultValue;
			};
		}

		void setStructType(StructType structType) {
			if (this.structType != null) {
				throw new IllegalStateException("Field %s is already part of a StructType".formatted(name()));
			}
			this.structType = structType;
		}

		public String name() {
			return name;
		}

		public String documentation() {
			return documentation;
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
			return Utils.recursionSafeEquals(this, o, Field::name, Field::documentation, Field::cardinality, Field::type, Field::defaultValue);
		}

		@Override
		public int hashCode() {
			return Utils.recursionSafeHashCode(this, name, documentation, cardinality, type, defaultValue);
		}
	}
}
