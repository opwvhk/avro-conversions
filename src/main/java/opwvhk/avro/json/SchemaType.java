package opwvhk.avro.json;

import java.util.Locale;

/**
 * Supported JSON schema types.
 */
public enum SchemaType {
	OBJECT, ARRAY, STRING, INTEGER, NUMBER, BOOLEAN, NULL;

	/**
	 * Determine the {@code SchemaType} for a type name in a JSON schema.
	 *
	 * @param name a type name as it occurs in a JSON schema
	 * @return the corresponding {@code SchemaType}
	 */
	public static SchemaType ofName(String name) {
		String upperCase = name.toUpperCase(Locale.ROOT);
		return SchemaType.valueOf(upperCase);
	}
}
