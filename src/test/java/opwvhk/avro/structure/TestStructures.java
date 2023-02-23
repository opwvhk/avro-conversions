package opwvhk.avro.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;
import static opwvhk.avro.structure.Cardinality.MULTIPLE;
import static opwvhk.avro.structure.Cardinality.OPTIONAL;
import static opwvhk.avro.structure.Cardinality.REQUIRED;

public class TestStructures {
	private static final ThreadLocal<StructType> LAST_STRUCT = new ThreadLocal<>();

	public static StructType struct(String name) {
		return struct(null, List.of(name), null);
	}

	public static StructType struct(String name, String doc) {
		return struct(null, List.of(name), doc);
	}

	public static StructType struct(List<String> nameAndAliases, String doc) {
		return struct(null, nameAndAliases, doc);
	}

	public static StructType struct(TypeCollection typeCollection, List<String> nameAndAliases, String doc) {
		TypeCollection typeCol = typeCollection != null ? typeCollection : new TypeCollection();
		StructType structType = new StructType(typeCol, nameAndAliases.get(0), nameAndAliases.subList(1, nameAndAliases.size()), doc);
		LAST_STRUCT.set(structType);
		return structType;
	}

	public static StructType.Field required(String name, Type type) {
		return required(name, null, type, null);
	}

	public static StructType.Field required(List<String> nameAndAliases, Type type) {
		return required(nameAndAliases, null, type, null);
	}

	public static StructType.Field required(String name, String doc, Type type, Object defaultValue) {
		return required(List.of(name), doc, type, defaultValue);
	}

	public static StructType.Field required(List<String> nameAndAliases, String doc, Type type, Object defaultValue) {
		return new StructType.Field(new ArrayList<>(nameAndAliases), doc, REQUIRED, type, defaultValue);
	}

	public static StructType.Field optional(String name, Type type) {
		return optional(name, null, type, null);
	}

	public static StructType.Field optional(String name, String doc, Type type, Object defaultValue) {
		return optional(List.of(name), doc, type, defaultValue);
	}

	public static StructType.Field optional(List<String> nameAndAliases, String doc, Type type, Object defaultValue) {
		return new StructType.Field(new ArrayList<>(nameAndAliases), doc, OPTIONAL, type, defaultValue);
	}

	public static StructType.Field array(String name, Type type) {
		return new StructType.Field(name, emptyList(), null, MULTIPLE, type, emptyList());
	}
}
