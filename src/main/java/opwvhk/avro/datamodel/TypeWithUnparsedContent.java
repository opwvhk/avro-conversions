package opwvhk.avro.datamodel;

public record TypeWithUnparsedContent(Type actualType) implements Type {

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
