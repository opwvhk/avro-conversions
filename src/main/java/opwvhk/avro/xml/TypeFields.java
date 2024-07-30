package opwvhk.avro.xml;

import opwvhk.avro.xml.datamodel.Cardinality;
import opwvhk.avro.xml.datamodel.FixedType;
import opwvhk.avro.xml.datamodel.ScalarType;
import opwvhk.avro.xml.datamodel.StructType;
import opwvhk.avro.xml.datamodel.Type;
import opwvhk.avro.xml.datamodel.TypeWithUnparsedContent;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

class TypeFields {
	private final Type recordType;
	private boolean shouldNotParseElements;
	private StructType.Field valueField;
	private final List<StructType.Field> attributeFields;
	private final List<StructType.Field> elementFields;

	TypeFields(TypeData typeData) {
		recordType = new StructType(typeData.name(), typeData.doc());
		this.shouldNotParseElements = typeData.shouldNotBeParsed();
		attributeFields = new ArrayList<>();
		elementFields = new ArrayList<>();
	}

	TypeFields(ScalarType recordType) {
		this.recordType = recordType;
		shouldNotParseElements = true;
		attributeFields = new ArrayList<>();
		elementFields = new ArrayList<>();
	}

	void shouldNotParseElements() {
		this.shouldNotParseElements = true;
	}

	boolean isScalarValue() {
		return recordType instanceof ScalarType;
	}

	void setValueField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
		this.valueField = createField(fieldCardinality, name, doc, type, defaultValue);
	}

	void addAttributeField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
		attributeFields.add(createField(fieldCardinality, name, doc, type, defaultValue));
	}

	void addElementField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
		elementFields.add(createField(fieldCardinality, name, doc, type, defaultValue));
	}

	private StructType.Field createField(Cardinality fieldCardinality, String name, String doc, Type type, Object defaultValue) {
		return new StructType.Field(name, doc, fieldCardinality, type, defaultValue);
	}

	private List<StructType.Field> fields() {
		Stream<StructType.Field> elementFieldStream;
		if (shouldNotParseElements) {
			elementFieldStream = Stream.of(
					new StructType.Field("value", "The entire element content, unparsed.", Cardinality.OPTIONAL, FixedType.STRING,
							StructType.Field.NULL_VALUE));
		} else if (valueField != null) {
			// The element has simple content, optionally with attributes
			elementFieldStream = Stream.of(valueField);
		} else {
			// The element contains fields
			elementFieldStream = elementFields.stream();
		}
		return Stream.concat(attributeFields.stream(), elementFieldStream).toList();
	}

	Type recordSchema() {
		return recordType;
	}

	Type completeRecordSchema() {
		// Note: MUST only be called once!
		if (recordType instanceof StructType structType) {
			structType.setFields(fields());
			return shouldNotParseElements ? new TypeWithUnparsedContent(recordType) : recordType;
		}
		return recordType;
	}


}
