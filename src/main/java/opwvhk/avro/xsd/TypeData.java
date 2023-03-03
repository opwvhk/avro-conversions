package opwvhk.avro.xsd;

import opwvhk.avro.util.Utils;

public record TypeData(String name, String doc, boolean shouldNotBeParsed) {
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
