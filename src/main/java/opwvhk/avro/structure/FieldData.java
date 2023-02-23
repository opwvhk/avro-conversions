package opwvhk.avro.structure;

import java.util.IdentityHashMap;

import static opwvhk.avro.util.Utils.truncate;

public record FieldData(String name, String doc, Cardinality cardinality, ScalarType scalarType, Object defaultValue) {

	public FieldData withName(String newName) {
		return new FieldData(newName, doc, cardinality, scalarType, defaultValue);
	}
	@Override
	public String toString() {
		String doc0 = truncate(10, doc);

		StringBuilder buffer = new StringBuilder(64);
		buffer.append(name).append(switch (cardinality) {
			case MULTIPLE -> "[]";
			case OPTIONAL -> "?";
			default -> "";
		});
		if (scalarType != null) {
			buffer.append(": ").append(scalarType.debugString(""));
			if (defaultValue != null) {
				buffer.append("=").append(defaultValue);
			}
		}
		if (doc0 != null) {
			buffer.append(" (").append(doc0).append(")");
		}
		return buffer.toString();
	}
}
