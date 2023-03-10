package opwvhk.avro.xml.datamodel;

import org.apache.avro.Schema;

/**
 * A type descriptor.
 */
public sealed interface Type permits ScalarType, StructType, TypeWithUnparsedContent {
	/**
	 * Create an Avro schema that matches the described type.
	 *
	 * @return an Avro schema
	 */
	Schema toSchema();

	/**
	 * Return an indented String describing this type, used for debugging. The indents ensure the resulting string can be used as-is, without manipulation.
	 *
	 * @param indent the indent to use; ideally whitespace only
	 * @return an indented String describing this type
	 */
	String debugString(String indent);
}
