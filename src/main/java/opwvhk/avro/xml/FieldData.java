package opwvhk.avro.xml;

import opwvhk.avro.xml.datamodel.Cardinality;
import opwvhk.avro.xml.datamodel.ScalarType;

import static opwvhk.avro.util.Utils.truncate;

record FieldData(String name, String doc, Cardinality cardinality, ScalarType scalarType, Object defaultValue) {

	public FieldData withName(String newName) {
		return new FieldData(newName, doc, cardinality, scalarType, defaultValue);
	}
	@Override
	public String toString() {
		String doc0 = truncate(10, doc);

		StringBuilder buffer = new StringBuilder(64);
		buffer.append(cardinality.formatName(name));
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
