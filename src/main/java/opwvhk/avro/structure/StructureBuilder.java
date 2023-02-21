package opwvhk.avro.structure;

import java.util.List;

/**
 * Interface to define schema builders with.
 *
 * <p>Methods in this interface are always called in a specific order, depending on whether a specific element type was already encountered:<ol>
 *
 * <li>{@link #startElement(FieldData, TypeData, List) startElement(FieldData, TypeData, List&lt;FieldData&gt;)}<br/>
 * Called for each <em>new</em> type. If the type has already been encountered, skip to
 * {@link #repeatedElement(FieldData, TypeData) repeatedElement(FieldData, TypeData)} instead. This prevents infinite recursion, but is used to skip
 * <strong>all</strong> repeated type definitions.</li>
 *
 * <li>This entire call sequence is nested for any element in the type definition.</li>
 *
 * <li>{@link #elementContainsAny(Object) elementContainsAny(ElementState)}<br/>
 * Called for each occurrence of 'any' tag in the (new) type definition.</li>
 *
 * <li>{@link #endElement(Object, FieldData, TypeData) endElement(ElementState, FieldData, TypeData)}<br/>
 * Called for each (new) element, to define the resulting schema.</li>
 *
 * <li>{@link #repeatedElement(FieldData, TypeData) repeatedElement(FieldData, TypeData)}<br/>
 * Called for each <em>repeated</em> type; skipped for new types: the methods above are called for those.</li>
 *
 * <li>{@link #element(Object, FieldData, TypeData, Object) element(ElementState, FieldData, TypeData, Result)}<br/>
 * Called after each nested call sequence, to embed the resulting schema in the parent element.</li>
 *
 * </ol></p>
 *
 * <p><strong>Preventing infinite recursion for recursive XML schemas</strong></p>
 *
 * <p>XML schemas can contain recursive definitions. To support this, type definitions are only traversed once. The first traversal gets all type
 * information, subsequent occurrences are short-circuited and only mark the use of the type for an element. This can also happen <em>while the type is still
 * being defined</em>.</p>
 *
 * <p>This means that when {@link #repeatedElement(FieldData, TypeData) repeatedElement(&hellip;)} is called, the corresponding
 * {@link #startElement(FieldData, TypeData, List) startElement(&hellip;)} is guaranteed to be called, but the corresponding
 * {@link #endElement(Object, FieldData, TypeData) endElement(&hellip;)} may not have been called.</p>
 *
 * <p>In short: you must be able to define results that (eventually) reference themselves.</p>
 *
 * @param <ElementState> The (mutable) element state to use while building a schema
 * @param <Result>       The resulting schema type
 */
public interface StructureBuilder<ElementState, Result> {
	/**
	 * Called when encountering an element with a new type in an XML schema.
	 *
	 * @param fieldData data describing the field (an XML tag)
	 * @param typeData  data describing the type (what content to expect inside the tag)
	 * @return the state for this element
	 */
	ElementState startElement(FieldData fieldData, TypeData typeData, List<FieldData> attributes);

	/**
	 * Called when there will be no (more) calls defining the (new) type content. Note that the type can be used in calls to
	 * {@link #repeatedElement(FieldData, TypeData) repeatedElement(&hellip;)} before this method is called.
	 *
	 * @param elementState the element state for the current element
	 * @param fieldData    data describing the field (an XML tag)
	 * @param typeData     data describing the type (what content to expect inside the tag)
	 * @return a schema describing the element (type)
	 */
	Result endElement(ElementState elementState, FieldData fieldData, TypeData typeData);

	/**
	 * <p>Called when encountering a (complex) type definition that has been seen before. Simple types (such as all standard XML schema types) do not cause a
	 * call to this method, as they can not be part of a recursive type loop.</p>
	 *
	 * <p>Note that the method {@link #startElement(FieldData, TypeData, List) startElement(&hellip;)} is guaranteed to have been called for the same type,
	 * but the corresponding {@link #endElement(Object, FieldData, TypeData) endElement(&hellip;)} may not. Even so, this method is expected to return the same
	 * (or an equivalent) result.</p>
	 *
	 * @param fieldData data describing the field (an XML tag)
	 * @param typeData  data describing the type (what content to expect inside the tag)
	 * @return a schema describing the element (type)
	 */
	Result repeatedElement(FieldData fieldData, TypeData typeData);

	/**
	 * Called after receiving a result for an element type, so it can be embedded in the parent type.
	 *
	 * <p>That is, this method is called directly after {@link #endElement(Object, FieldData, TypeData) endElement(&hellip;)} or
	 * {@link #repeatedElement(FieldData, TypeData)} repeatedElement(&hellip;)} is called.</p>
	 *
	 * @param parentElementState the element state for the parent element
	 * @param fieldData          data describing the field (an XML tag)
	 * @param typeData           data describing the type (what content to expect inside the tag)
	 * @param elementResult      the schema for the (type)
	 */
	void element(ElementState parentElementState, FieldData fieldData, TypeData typeData, Result elementResult);

	/**
	 * Called when encountering an 'any' tag inside an XML element definition
	 *
	 * @param parentElementState the element state for the element that contains any tag
	 */
	void elementContainsAny(ElementState parentElementState);
}
