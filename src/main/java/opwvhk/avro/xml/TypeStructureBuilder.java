package opwvhk.avro.xml;

import opwvhk.avro.xml.datamodel.FixedType;
import opwvhk.avro.xml.datamodel.ScalarType;
import opwvhk.avro.xml.datamodel.StructType;
import opwvhk.avro.xml.datamodel.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * <p>Methods in this class are always called in a specific order, depending on whether a specific element type was already encountered:</p>
 *
 * <ol>
 *
 * <li>{@link #startElement(FieldData, TypeData, List) startElement(FieldData, TypeData, List&lt;FieldData&gt;)}<br/>
 * Called for each <em>new</em> type. If the type has already been encountered, skip to
 * {@link #repeatedElement(TypeData) repeatedElement(FieldData, TypeData)} instead. This prevents infinite recursion, but is used to skip
 * <strong>all</strong> repeated type definitions.</li>
 *
 * <li>This entire call sequence is nested for any element in the type definition.</li>
 *
 * <li>{@link #elementContainsAny(TypeFields)}<br/>
 * Called for each occurrence of 'any' tag in the (new) type definition.</li>
 *
 * <li>{@link #endElement(TypeFields)}<br/>
 * Called for each (new) element, to define the resulting schema.</li>
 *
 * <li>{@link #repeatedElement(TypeData) repeatedElement(FieldData, TypeData)}<br/>
 * Called for each <em>repeated</em> type; skipped for new types: the methods above are called for those.</li>
 *
 * <li>{@link #element(TypeFields, FieldData, Type)}<br/>
 * Called after each nested call sequence, to embed the resulting schema in the parent element.</li>
 *
 * </ol>
 *
 * <p><strong>Preventing infinite recursion for recursive XML schemas</strong></p>
 *
 * <p>XML schemas can contain recursive definitions. To support this, type definitions are only traversed once. The first traversal gets all type
 * information, subsequent occurrences are short-circuited and only mark the use of the type for an element. This can also happen <em>while the type is still
 * being defined</em>.</p>
 *
 * <p>This means that when {@link #repeatedElement(TypeData) repeatedElement(&hellip;)} is called, the corresponding
 * {@link #startElement(FieldData, TypeData, List) startElement(&hellip;)} is guaranteed to be called, but the corresponding
 * {@link #endElement(TypeFields) endElement(&hellip;)} may not have been called.</p>
 *
 * <p>In short: you must be able to define results that (eventually) reference themselves.</p>
 */
class TypeStructureBuilder {
	/**
	 * Schemas by full name. Note that these schemas need not be records!
	 */
	private final Map<String, Type> definedSchemasByFullname = new HashMap<>();

	/**
	 * Called when encountering an element with a new type in an XML schema.
	 *
	 * @param fieldData data describing the field (an XML tag)
	 * @param typeData  data describing the type (what content to expect inside the tag)
	 * @return the state for this element
	 */
	TypeFields startElement(FieldData fieldData, TypeData typeData, List<FieldData> attributes) {
		// If a type is complete, it will not contain any elements. The next method call will be to endElement(...).

		String className = typeData.name();

		TypeFields fields;
		// If typeData.shouldNotBeParsed(), then the type must be complex and fieldData.simpleType() == null
		if (attributes.isEmpty() && fieldData.scalarType() != null) {
			// Scalar element without attributes: behave like attribute
			fields = new TypeFields(fieldData.scalarType());
		} else if (attributes.isEmpty() && typeData.shouldNotBeParsed()) {
			// Unparsed element without attributes: behave like attribute
			fields = new TypeFields(FixedType.STRING);
		} else {
			// The element has attributes, elements, or both
			fields = new TypeFields(typeData);
			for (FieldData attributeData : attributes) {
				ScalarType scalarType = requireNonNull(attributeData.scalarType());
				fields.addAttributeField(attributeData.cardinality(), attributeData.name(), attributeData.doc(), scalarType,
						attributeData.defaultValue());
			}
			if (fieldData.scalarType() != null) {
				FieldData fieldData1 = fieldData.withName("value");
				ScalarType scalarType = requireNonNull(fieldData1.scalarType());
				fields.setValueField(fieldData1.cardinality(), fieldData1.name(), fieldData1.doc(), scalarType, fieldData1.defaultValue());
			}
		}

		if (className != null) {
			definedSchemasByFullname.put(className, fields.recordSchema());
		}

		return fields;
	}

	/**
	 * Called when there will be no (more) calls defining the (new) type content. Note that the type can be used in calls to
	 * {@link #repeatedElement(TypeData) repeatedElement(&hellip;)} before this method is called.
	 *
	 * @param typeFields the fields for the current element/type
	 * @return a schema describing the element (type)
	 */
	Type endElement(TypeFields typeFields) {
		return typeFields.completeRecordSchema();
	}

	/**
	 * <p>Called when encountering a (complex) type definition that has been seen before. Simple types (such as all standard XML schema types) do not cause a
	 * call to this method, as they can not be part of a recursive type loop.</p>
	 *
	 * <p>Note that the method {@link #startElement(FieldData, TypeData, List) startElement(&hellip;)} is guaranteed to have been called for the same type,
	 * but the corresponding {@link #endElement(TypeFields) endElement(&hellip;)} may not. Even so, this method is expected to return the same
	 * (or an equivalent) result.</p>
	 *
	 * @param typeData data describing the type (what content to expect inside the tag)
	 * @return a schema describing the element (type)
	 */
	Type repeatedElement(TypeData typeData) {
		String className = typeData.name();
		return definedSchemasByFullname.get(className);
	}

	/**
	 * Called after receiving a result for an element type, so it can be embedded in the parent type.
	 *
	 * <p>That is, this method is called directly after {@link #endElement(TypeFields) endElement(&hellip;)} or
	 * {@link #repeatedElement(TypeData)} repeatedElement(&hellip;)} is called.</p>
	 *
	 * @param parentTypeFields the fields for the parent element/type
	 * @param fieldData        data describing the field (an XML tag)
	 * @param elementResult    the schema for the (type)
	 */
	void element(TypeFields parentTypeFields, FieldData fieldData, Type elementResult) {
		if (parentTypeFields.isScalarValue()) {
			return;
		}

		Object defaultValue = elementResult instanceof StructType ? null : fieldData.defaultValue();
		parentTypeFields.addElementField(fieldData.cardinality(), fieldData.name(), fieldData.doc(), elementResult, defaultValue);
	}

	/**
	 * Called when encountering an 'any' tag inside an XML element definition.
	 *
	 * @param parentTypeFields the fields for the element that contains any tag
	 */
	void elementContainsAny(TypeFields parentTypeFields) {
		parentTypeFields.shouldNotParseElements();
	}
}
