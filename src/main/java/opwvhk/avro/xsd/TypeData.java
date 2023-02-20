package opwvhk.avro.xsd;

import opwvhk.avro.Utils;

import static opwvhk.avro.Utils.first;

public record TypeData(String name, String doc, boolean shouldNotBeParsed) {
	public TypeData extend(FieldData fieldData) {
		return new TypeData(first(name, fieldData.name()), first(doc, fieldData.doc()), shouldNotBeParsed);
	}

	@Override
	public String toString() {
		String doc0 = Utils.truncate(10, doc);

		StringBuilder buffer = new StringBuilder();
		buffer.append(name == null ? "anonymous" : name);
		if (shouldNotBeParsed || doc0 != null) {
			buffer.append(" (");
		}
		if (shouldNotBeParsed) {
			buffer.append("mixed");
		}
		if (shouldNotBeParsed && doc0 != null) {
			buffer.append("; ");
		}
		if (doc0 != null) {
			buffer.append(doc0);
		}
		if (shouldNotBeParsed || doc0 != null) {
			buffer.append(")");
		}
		return buffer.toString();
	}
}
