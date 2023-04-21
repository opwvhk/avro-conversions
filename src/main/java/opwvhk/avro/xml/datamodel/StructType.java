package opwvhk.avro.xml.datamodel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import opwvhk.avro.util.Utils;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static opwvhk.avro.util.Utils.nonRecursive;
import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.apache.avro.Schema.Type.NULL;

/**
 * A structural type with named fields.
 */
public final class StructType implements Type {
    private final String name;
    private final String documentation;
    private List<Field> fields;

    /**
     * Create a {@code StructType}.
     *
     * @param name          the (full) name of the type
     * @param documentation any documentation describing the type
     */
    public StructType(String name, String documentation) {
        this.name = name;
        this.documentation = documentation;
        fields = null;
    }

    /**
     * Returns the fields defined for this type.
     *
     * @return the list of fields, or {@code null} if they haven't been defined yet
     */
    public List<Field> fields() {
        return fields;
    }

    /**
     * Set the given fields on this structural type, and return it. Fields can be set only once.
     *
     * @param fields the fields to set on the structural type
     * @return this instance
     * @see #setFields(List) setFields(List&lt;Field&gt;)
     */
    public StructType withFields(Field... fields) {
        setFields(asList(fields));
        return this;
    }

    /**
     * Set the given list of fields on this structural type. Fields can be set only once.
     *
     * @param fields the fields to set on the structural type
     * @see #withFields(Field...)
     */
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
        this.fields = List.copyOf(fields);
    }

    @Override
    public Schema toSchema() {
        List<Schema.Field> avroFields = this.fields.stream().map(f -> {
            Schema fieldSchema = f.type().toSchema();
            Object defaultValue = f.defaultValue();
            if (f.cardinality() == Cardinality.MULTIPLE) {
                fieldSchema = Schema.createArray(fieldSchema);
            } else if (f.cardinality() == Cardinality.OPTIONAL) {
                if (defaultValue != StructType.Field.NULL_VALUE && defaultValue != JsonProperties.NULL_VALUE && defaultValue != NULL_DEFAULT_VALUE) {
                    fieldSchema = Schema.createUnion(fieldSchema, Schema.create(NULL));
                } else {
                    fieldSchema = Schema.createUnion(Schema.create(NULL), fieldSchema);
                }
            }
            return new Schema.Field(f.name(), fieldSchema, f.documentation(),
                    defaultValue == StructType.Field.NULL_VALUE ? NULL_DEFAULT_VALUE : defaultValue);
        }).toList();
        return Schema.createRecord(name, documentation, null, false, avroFields);
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
        //noinspection NonFinalFieldReferenceInEquals
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
                           buf.append("\n  ").append(indent).append(field.cardinality().formatName(field.name()));
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

    /**
     * A field in a structural type.
     */
    public static final class Field {
        /**
         * Constant to use to denote a {@code null} default value (as {@code null} means "no default value").
         */
        public static final Object NULL_VALUE = new Object();
        private StructType structType;
        private final String name;
        private final String documentation;
        private final Cardinality cardinality;
        private final Type type;
        private final Object defaultValue;

        /**
         * Create a field.
         *
         * @param name          the name of the field
         * @param documentation documentation describing the field, if any
         * @param cardinality   the cardinality of the field
         * @param type          the field type
         * @param defaultValue  a default value to use, if any
         */
        public Field(String name, String documentation, Cardinality cardinality, Type type, Object defaultValue) {
            structType = null;
            this.name = requireNonNull(name, "Cannot create field without a name.");
            this.documentation = documentation;
            this.cardinality = requireNonNull(cardinality, "Cannot create field without cardinality.");
            this.type = requireNonNull(type, "Cannot create field without a type.");
            this.defaultValue = switch (cardinality) {
                // Override default value for arrays: nulls cause problems, amd anything else doesn't make sense.
                case MULTIPLE -> emptyList();
                // Optional fields must have a default, as 'optional' applies both to the presence of a value and to whether they must be set.
                case OPTIONAL -> Optional.ofNullable(defaultValue).orElse(StructType.Field.NULL_VALUE);
                default -> defaultValue;
            };
        }

        private void setStructType(StructType structType) {
            if (this.structType != null) {
                throw new IllegalStateException("Field %s is already part of a StructType".formatted(name()));
            }
            this.structType = structType;
        }

        /**
         * Return the name of the field.
         *
         * @return the field name
         */
        public String name() {
            return name;
        }

        private String documentation() {
            return documentation;
        }

        /**
         * Return the field cardinality.
         *
         * @return the cardinality
         */
        public Cardinality cardinality() {
            return cardinality;
        }

        /**
         * Return the field type.
         *
         * @return the field type
         */
        public Type type() {
            return type;
        }

        private Object defaultValue() {
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
