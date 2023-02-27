package opwvhk.avro.datamodel;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static opwvhk.avro.datamodel.Cardinality.MULTIPLE;
import static opwvhk.avro.datamodel.Cardinality.OPTIONAL;
import static opwvhk.avro.datamodel.Cardinality.REQUIRED;

public class TestStructures {
	private static final ThreadLocal<StructType> LAST_STRUCT = new ThreadLocal<>();

	public static EnumType enumType(String name, String documentation, List<String> enumSymbols) {
		return new EnumType(new TypeCollection(), name, documentation, enumSymbols, null);
	}

	public static StructType struct(String name) {
		return struct(null, name, Set.of(), null);
	}

	public static StructType struct(String name, String doc) {
		return struct(null, name, Set.of(), doc);
	}

	public static StructType struct(String name, Set<String> aliases, String doc) {
		return struct(null, name, aliases, doc);
	}

	public static StructType struct(TypeCollection typeCollection, String name, Set<String> aliases, String doc) {
		TypeCollection typeCol = typeCollection != null ? typeCollection : new TypeCollection();
		StructType structType = new StructType(typeCol, name, aliases, doc);
		LAST_STRUCT.set(structType);
		return structType;
	}

	public static StructType.Field required(String name, Type type) {
		return required(name, null, type, null);
	}

	public static StructType.Field required(String name, Set<String> aliases, Type type) {
		return required(name, aliases, null, type, null);
	}

	public static StructType.Field required(String name, String doc, Type type, Object defaultValue) {
		return required(name, Set.of(), doc, type, defaultValue);
	}

	public static StructType.Field required(String name, Set<String> aliases, String doc, Type type, Object defaultValue) {
		return new StructType.Field(name, aliases, doc, REQUIRED, type, defaultValue);
	}

	public static StructType.Field optional(String name, Type type) {
		return optional(name, null, type, null);
	}

	public static StructType.Field optional(String name, String doc, Type type, Object defaultValue) {
		return optional(name, Set.of(), doc, type, defaultValue);
	}

	public static StructType.Field optional(String name, Set<String> aliases, String doc, Type type, Object defaultValue) {
		return new StructType.Field(name, aliases, doc, OPTIONAL, type, defaultValue);
	}

	public static StructType.Field array(String name, Type type) {
		return new StructType.Field(name, emptySet(), null, MULTIPLE, type, emptyList());
	}
}
