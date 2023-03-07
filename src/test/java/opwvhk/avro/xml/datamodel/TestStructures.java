package opwvhk.avro.xml.datamodel;

import java.util.List;

import static java.util.Collections.emptyList;
import static opwvhk.avro.xml.datamodel.Cardinality.MULTIPLE;
import static opwvhk.avro.xml.datamodel.Cardinality.OPTIONAL;
import static opwvhk.avro.xml.datamodel.Cardinality.REQUIRED;

public class TestStructures {
	private static final ThreadLocal<StructType> LAST_STRUCT = new ThreadLocal<>();

	public static EnumType enumType(String name, String documentation, List<String> enumSymbols) {
		return new EnumType(name, documentation, enumSymbols, null);
	}

	public static EnumType enumType(String name, List<String> enumSymbols, String defaultSymbol) {
		return new EnumType(name, null, enumSymbols, defaultSymbol);
	}

	public static Type unparsed(Type type) {
		return new TypeWithUnparsedContent(type);
	}

	public static StructType struct(String name) {
		return struct(name, null);
	}

	public static StructType struct(String name, String doc) {
		StructType structType = new StructType(name, doc);
		LAST_STRUCT.set(structType);
		return structType;
	}

	public static StructType.Field required(String name, Type type) {
		return required(name, null, type, null);
	}

	public static StructType.Field required(String name, String doc, Type type, Object defaultValue) {
		return new StructType.Field(name, doc, REQUIRED, type, defaultValue);
	}

	public static StructType.Field optional(String name, Type type) {
		return optional(name, null, type, null);
	}

	public static StructType.Field optional(String name, String doc, Type type, Object defaultValue) {
		return new StructType.Field(name, doc, OPTIONAL, type, defaultValue);
	}

	public static StructType.Field array(String name, Type type) {
		return new StructType.Field(name, null, MULTIPLE, type, emptyList());
	}
}
