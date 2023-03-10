package opwvhk.avro.xml.datamodel;

import org.apache.avro.Schema;

/**
 * A type with unparsed content. This means the type has a text field named "value", where the XML  parser can place the content of an XML element.
 *
 * @param actualType the actual type (with a text field named "value")
 */
public record TypeWithUnparsedContent(Type actualType) implements Type {
	@Override
	public Schema toSchema() {
		return actualType.toSchema();
	}

	@Override
	public String toString() {
		return "(unparsed) " + actualType.debugString("");
	}

	@Override
	public String debugString(String indent) {
		String debugString = actualType.debugString(indent);
		int insertPosition = indent.length();
		return debugString.substring(0, insertPosition) + "(unparsed) " + debugString.substring(insertPosition);
	}
}
