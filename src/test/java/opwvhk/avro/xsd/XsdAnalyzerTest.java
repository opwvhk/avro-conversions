package opwvhk.avro.xsd;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.List;
import java.util.Objects;

import opwvhk.avro.datamodel.DecimalType;
import opwvhk.avro.structure.FieldData;
import opwvhk.avro.datamodel.StructType;
import opwvhk.avro.structure.StructureBuilder;
import opwvhk.avro.datamodel.Type;
import opwvhk.avro.structure.TypeData;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.walker.XmlSchemaAttrInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaTypeInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.Test;

import static opwvhk.avro.datamodel.Cardinality.REQUIRED;
import static opwvhk.avro.datamodel.FixedType.BINARY_BASE64;
import static opwvhk.avro.datamodel.FixedType.BINARY_HEX;
import static opwvhk.avro.datamodel.FixedType.BOOLEAN;
import static opwvhk.avro.datamodel.FixedType.DATE;
import static opwvhk.avro.datamodel.FixedType.DATETIME;
import static opwvhk.avro.datamodel.FixedType.DOUBLE;
import static opwvhk.avro.datamodel.FixedType.FLOAT;
import static opwvhk.avro.datamodel.FixedType.STRING;
import static opwvhk.avro.datamodel.FixedType.TIME;
import static opwvhk.avro.datamodel.TestStructures.array;
import static opwvhk.avro.datamodel.TestStructures.enumType;
import static opwvhk.avro.datamodel.TestStructures.optional;
import static opwvhk.avro.datamodel.TestStructures.required;
import static opwvhk.avro.datamodel.TestStructures.struct;
import static opwvhk.avro.datamodel.TestStructures.unparsed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class XsdAnalyzerTest {
	private static XsdAnalyzer analyzer;

	@Before
	public void setUp()
			throws Exception {
		URL StructTypeUrl = XsdAnalyzerTest.class.getResource("/testCases.xsd");
		analyzer = new XsdAnalyzer(Objects.requireNonNull(StructTypeUrl));
		analyzer.mapTargetNamespace("namespace");
	}

	@Test
	public void checkTestNamespaces() {
		assertThat(analyzer.availableNamespaces()).containsExactly("https://www.schiphol.nl/avro-tools/tests");
	}

	@Test
	public void checkXmlNamespaceIsCorrectlySpecified()
			throws IOException {
		assertThatThrownBy(() -> analyzer.typeOf(new QName("unknown", "unknown"))).isInstanceOf(IllegalArgumentException.class);

		URL StructTypeUrl = XsdAnalyzerTest.class.getResource("/testCases.xsd");
		XsdAnalyzer extraAnalyzer = new XsdAnalyzer(Objects.requireNonNull(StructTypeUrl));
		extraAnalyzer.mapNamespace("https://www.schiphol.nl/avro-tools/tests", "namespace");
		extraAnalyzer.mapNamespace("ExtraNameSpace", "extra");
		assertThatThrownBy(() -> extraAnalyzer.typeOf("unknown")).isInstanceOf(IllegalArgumentException.class);
	}

	/*
	 * Happy flow variations.
	 */

	@Test
	public void groupedStructuresAreHandledAndDocumentedCorrectly() {
		Type type = analyzer.typeOf(new QName("https://www.schiphol.nl/avro-tools/tests", "GroupStructures"));
		StructType keep = struct("namespace.Keep").withFields(
				required("value", STRING)
		);
		StructType group = struct("namespace.group", "This documents both the field and the record").withFields(
				array("one", STRING),
				array("other", STRING),
				required("Keep", keep)
		);
		assertThat(type).isEqualTo(struct("namespace.GroupStructures",
				"Record documentation is taken from the type if possible, from the element otherwise").withFields(
				optional("comment", "A comment describing the group; can be placed before or after it in the XML.", STRING, null),
				required("group", "This documents both the field and the record", group, null)
		));
	}

	@Test
	public void attributesAreSupported() {
		Type type = analyzer.typeOf("AttributesAndAnnotationWithoutDocs");

		assertThat(type).isEqualTo(struct("namespace.AttributesAndAnnotationWithoutDocs").withFields(
				required("id", DecimalType.INTEGER_TYPE),
				optional("something", DecimalType.INTEGER_TYPE)
		));
	}

	@Test
	public void attributesWithSimpleContentAreSupported() {
		Type type = analyzer.typeOf("ExtensionInSimpleContent");

		assertThat(type).isEqualTo(struct("namespace.ExtensionInSimpleContent").withFields(
				required("value", STRING),
				optional("version", STRING)
		));
	}

	@Test
	public void attributesWithComplexContentAreSupported() {
		Type type = analyzer.typeOf("ExtensionInMixedComplexContent");
		assertThat(type).isEqualTo(struct("namespace.ExtensionInMixedComplexContent").withFields(
				required("value", STRING),
				optional("version", STRING)
		));
	}

	@Test
	public void repeatedNestedValuesBecomeArrays() {
		Type type = analyzer.typeOf("RepeatedNestedRecordWithOptionalField");

		StructType array = struct("namespace.array").withFields(
				required("one", STRING),
				optional("two", STRING)
		);
		assertThat(type).isEqualTo(struct("namespace.RepeatedNestedRecordWithOptionalField").withFields(
				required("ID", STRING),
				array("array", array)
		));
	}

	@Test
	public void repeatedSequencedElementsBecomeArrays() {
		Type type = analyzer.typeOf("RepeatedSequence");

		StructType namedStructType = struct("namespace.named").withFields(
				required("description", STRING),
				required("name", STRING)
		);
		assertThat(type).isEqualTo(struct("namespace.RepeatedSequence").withFields(
				array("array1", namedStructType),
				array("array2", namedStructType)
		));
	}

	@Test
	public void repeatedChoiceElementsBecomeArrays() {
		Type type = analyzer.typeOf("RepeatedChoice");

		assertThat(type).isEqualTo(struct("namespace.RepeatedChoice").withFields(
				array("value", STRING)
		));
	}

	@Test
	public void optionalAllMakesElementsNullable() {
		Type type = analyzer.typeOf("OptionalAll");

		assertThat(type).isEqualTo(struct("namespace.OptionalAll").withFields(
				optional("value1", STRING),
				optional("value2", STRING)
		));
	}

	@Test
	public void allowsRestrictionInSimpleContent() {
		Type type = analyzer.typeOf("RestrictionInSimpleContent");
		assertThat(type).isEqualTo(STRING);
	}

	@Test
	public void allowsRestrictionInComplexContent() {
		Type type = analyzer.typeOf("RestrictionInComplexContent");
		assertThat(type).isEqualTo(struct("namespace.RestrictionInComplexContent").withFields(
				required("name", STRING)));
	}

	@Test
	public void allowsExtensionWithElements() {
		Type type = analyzer.typeOf("ExtensionWithElements");
		assertThat(type).isEqualTo(struct("namespace.ExtensionWithElements").withFields(
				required("description", STRING),
				required("field", STRING),
				required("name", STRING)
		));
	}

	@Test
	public void allowsExtensionOfComplexType() {
		Type type = analyzer.typeOf("ExtensionOfComplexType");
		assertThat(type).isEqualTo(struct("namespace.ExtensionOfComplexType").withFields(
				required("value", STRING),
				optional("version", STRING)
		));
	}

	@Test
	public void arbitraryXmlDataIsReadAsString() {
		Type expected = unparsed(struct("namespace.ArbitraryContent").withFields(
				required("source", STRING),
				optional("value", "The entire element content, unparsed.", STRING, null)
		));
		assertThat(analyzer.typeOf("ArbitraryContent")).isEqualTo(expected);
	}

	@Test
	public void mixedComplexTypesAreCoercedToString() {
		StructType expected = struct("namespace.MixedComplexType").withFields(
				required("source", STRING),
				required("Payload", STRING)
		);

		assertThat(analyzer.typeOf("MixedComplexType")).isEqualTo(expected);
	}

	@Test
	public void mixedComplexContentTreatedAsNormal() {
		Type type = analyzer.typeOf("MixedExtensionWithElements");
		assertThat(type).isEqualTo(
				struct("namespace.MixedExtensionWithElements", "Note that the complexContent being mixed does not affect the outcome!").withFields(
						required("description", STRING),
						required("field", STRING),
						required("name", STRING)
				));
	}

	@Test
	public void defaultValuesAreAddedIfPossible() {
		// Note: because we have a default value, the field becomes required (as there's always a value).
		// Reason: nil and absent values are treated equally, and mean "there's no value in the XML, so use the default"
		StructType optional = struct("namespace.optional").withFields(
				required("optimizedAway", null, STRING, "def")
		);
		StructType defaultToNull = struct("namespace.defaultToNull").withFields(
				optional("optimizedAway", STRING)
		);
		assertThat(analyzer.typeOf("DefaultValuesForFields")).isEqualTo(struct("namespace.DefaultValuesForFields").withFields(
				required("req", null, STRING, "ghi"),
				optional("opt", null, STRING, "jkl"),
				required("required", null, STRING, "abc"),
				optional("optional", null, optional, null),
				optional("defaultToNull", defaultToNull),
				array("array", STRING)
		));
	}

	@Test
	public void recursionIsAllowed() {
		StringWriter buffer = new StringWriter();
		analyzer.walkSchemaInTargetNamespace("Recursive", new XmlSchemaVisitor() {
			private final PrintWriter output = new PrintWriter(buffer);
			private final Indent indent = new Indent("  ");

			@Override
			public void onEnterElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
				output.printf("%s%sonEnterElement %s%s%n", indent.inc(), b ? "(" : "", e(xmlSchemaElement), b ? ")" : "");
			}

			private String e(XmlSchemaElement element) {
				return e(element.getQName().toString(), element);
			}

			private String e(String particleName, XmlSchemaParticle particle) {
				return String.format("(occurs %d-%d) %s",
						particle.getMinOccurs(), particle.getMaxOccurs(),
						particleName);
			}

			@Override
			public void onExitElement(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo, boolean b) {
				output.printf("%s%sonExitElement %s%s%n", indent.dec(), b ? "(" : "", e(xmlSchemaElement), b ? ")" : "");
			}

			@Override
			public void onVisitAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAttrInfo xmlSchemaAttrInfo) {
				output.printf("%sonVisitAttribute %s %s%n", indent, xmlSchemaAttrInfo.getAttribute().getQName(), xmlSchemaAttrInfo.getType().toString());
			}

			@Override
			public void onEndAttributes(XmlSchemaElement xmlSchemaElement, XmlSchemaTypeInfo xmlSchemaTypeInfo) {
				output.printf("%sonEndAttributes %s%n", indent, e(xmlSchemaElement));
			}

			@Override
			public void onEnterSubstitutionGroup(XmlSchemaElement xmlSchemaElement) {
				output.printf("%sonEnterSubstitutionGroup %s%n", indent.inc(), e(xmlSchemaElement));
			}

			@Override
			public void onExitSubstitutionGroup(XmlSchemaElement xmlSchemaElement) {
				output.printf("%sonExitSubstitutionGroup %s%n", indent.dec(), e(xmlSchemaElement));
			}

			@Override
			public void onEnterAllGroup(XmlSchemaAll xmlSchemaAll) {
				output.printf("%sonEnterAllGroup %s%n", indent.inc(), e("all", xmlSchemaAll));
			}

			@Override
			public void onExitAllGroup(XmlSchemaAll xmlSchemaAll) {
				output.printf("%sonExitAllGroup %s%n", indent.dec(), e("all", xmlSchemaAll));
			}

			@Override
			public void onEnterChoiceGroup(XmlSchemaChoice xmlSchemaChoice) {
				output.printf("%sonEnterChoiceGroup %s%n", indent.inc(), e("choice", xmlSchemaChoice));
			}

			@Override
			public void onExitChoiceGroup(XmlSchemaChoice xmlSchemaChoice) {
				output.printf("%sonExitChoiceGroup %s%n", indent.dec(), e("choice", xmlSchemaChoice));
			}

			@Override
			public void onEnterSequenceGroup(XmlSchemaSequence xmlSchemaSequence) {
				output.printf("%sonEnterSequenceGroup %s%n", indent.inc(), e("sequence", xmlSchemaSequence));
			}

			@Override
			public void onExitSequenceGroup(XmlSchemaSequence xmlSchemaSequence) {
				output.printf("%sonExitSequenceGroup %s%n", indent.dec(), e("sequence", xmlSchemaSequence));
			}

			@Override
			public void onVisitAny(XmlSchemaAny xmlSchemaAny) {
				output.printf("%sonVisitAny %s%n", indent, e("any", xmlSchemaAny));
			}

			@Override
			public void onVisitAnyAttribute(XmlSchemaElement xmlSchemaElement, XmlSchemaAnyAttribute xmlSchemaAnyAttribute) {
				output.printf("%sonVisitAnyAttribute%n", indent);
			}
		});
		assertThat(buffer.toString()).isEqualTo("""
				onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}Recursive
				  onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}Recursive
				  onEnterSequenceGroup (occurs 1-1) sequence
				    onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}HoleInTheGround
				      onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}HoleInTheGround
				      onEnterSequenceGroup (occurs 1-1) sequence
				        onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}level
				          onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}level
				        onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}level
				        (onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}RabbitHole)
				        (onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}RabbitHole)
				      onExitSequenceGroup (occurs 1-1) sequence
				    onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}HoleInTheGround
				    (onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}Recursive)
				    (onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}Recursive)
				    onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}WrappedStringArray
				      onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}WrappedStringArray
				      onEnterSequenceGroup (occurs 1-1) sequence
				        onEnterElement (occurs 0-9223372036854775807) {https://www.schiphol.nl/avro-tools/tests}Array
				          onEndAttributes (occurs 0-9223372036854775807) {https://www.schiphol.nl/avro-tools/tests}Array
				          onEnterSequenceGroup (occurs 1-1) sequence
				            onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}StringElement
				              onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}StringElement
				            onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}StringElement
				          onExitSequenceGroup (occurs 1-1) sequence
				        onExitElement (occurs 0-9223372036854775807) {https://www.schiphol.nl/avro-tools/tests}Array
				      onExitSequenceGroup (occurs 1-1) sequence
				    onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}WrappedStringArray
				    onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}WrappedNumberArray
				      onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}WrappedNumberArray
				      onEnterSequenceGroup (occurs 1-1) sequence
				        onEnterElement (occurs 0-9223372036854775807) {https://www.schiphol.nl/avro-tools/tests}Array
				          onVisitAttribute {https://www.schiphol.nl/avro-tools/tests}length XmlSchemaTypeInfo [ATOMIC] Base Type: DECIMAL User Recognized Type: {http://www.w3.org/2001/XMLSchema}int Is Mixed: false Num Children: 0
				          onEndAttributes (occurs 0-9223372036854775807) {https://www.schiphol.nl/avro-tools/tests}Array
				          onEnterSequenceGroup (occurs 1-1) sequence
				            onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}NumberElement
				              onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}NumberElement
				            onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}NumberElement
				          onExitSequenceGroup (occurs 1-1) sequence
				        onExitElement (occurs 0-9223372036854775807) {https://www.schiphol.nl/avro-tools/tests}Array
				      onExitSequenceGroup (occurs 1-1) sequence
				    onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}WrappedNumberArray
				  onExitSequenceGroup (occurs 1-1) sequence
				onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}Recursive
				""");
	}

	@Test
	public void recursionYieldsStructTypesAsWell() {
		StructType recursiveComplexType = struct("namespace.RecursiveComplexType");
		recursiveComplexType.setFields(List.of(
				required("level", DecimalType.INTEGER_TYPE),
				required("RabbitHole", recursiveComplexType)
		));
		StructType array = struct("namespace.Array").withFields(
				required("StringElement", STRING)
		);
		StructType wrappedStringArray = struct("namespace.WrappedStringArray").withFields(
				array("Array", array)
		);
		StructType array2 = struct("namespace.Array2").withFields(
				optional("length", DecimalType.INTEGER_TYPE),
				required("NumberElement", DecimalType.INTEGER_TYPE)
		);
		StructType wrappedNumberArray = struct("namespace.WrappedNumberArray").withFields(
				array("Array", array2)
		);
		StructType recursiveType = struct("namespace.Recursive");
		recursiveType.setFields(List.of(
				required("HoleInTheGround", recursiveComplexType),
				required("Recursive", recursiveType),
				required("WrappedStringArray", wrappedStringArray),
				required("WrappedNumberArray", wrappedNumberArray)
		));

		Type type = analyzer.typeOf("Recursive");
		assertThat(type).isEqualTo(recursiveType);
	}

	/*
	 * Edge cases of internal methods, etc.
	 */

	@Test
	public void validateClassNameUniqueness() {
		StructType normal = struct("namespace.Normal").withFields(
				required("field", STRING)
		);
		StructType typeName = struct("namespace.TypeName").withFields(
				required("Normal", normal)
		);
		StructType typeName2 = struct("namespace.TypeName2").withFields(
				required("name", STRING)
		);
		StructType normal2 = struct("namespace.Normal2").withFields(
				required("TheAnswer", DecimalType.INTEGER_TYPE)
		);
		StructType classNamesEdgeCases2 = struct("namespace.ClassNamesEdgeCases2").withFields(
				required("description", STRING),
				required("Normal", normal2)
		);
		StructType normal3 = struct("namespace.Normal3").withFields(
				required("field1", STRING),
				required("field2", STRING)
		);
		assertThat(analyzer.typeOf("ClassNamesEdgeCases")).isEqualTo(struct("namespace.ClassNamesEdgeCases").withFields(
				required("PlainType", typeName),
				required("AnotherType", typeName),
				required("TypeName", typeName2),
				required("ClassNamesEdgeCases", classNamesEdgeCases2),
				required("Normal", normal3)
		));
	}

	@Test
	public void checkMaximumNumberOfDuplicateNames() {
		// Duplicates
		assertThatThrownBy(() -> analyzer.walkSchemaInTargetNamespace("Duplicates", new StructuralSchemaVisitor<>(new StructureBuilder<>() {
			@Override
			public Object startElement(FieldData fieldData, TypeData typeData, List<FieldData> attributes) {
				return null;
			}

			@Override
			public Object endElement(Object o, FieldData fieldData, TypeData typeData) {
				return null;
			}

			@Override
			public Object repeatedElement(FieldData fieldData, TypeData typeData) {
				return null;
			}

			@Override
			public void element(Object parentElementState, FieldData fieldData, TypeData typeData, Object elementResult) {

			}

			@Override
			public void elementContainsAny(Object parentElementState) {

			}
		}, s -> s, 2))).isInstanceOf(IllegalStateException.class);
	}


	/*
	 * Unsupported structure variations.
	 */

	@Test
	public void failsIfElementNotFound() {
		assertFailureCreatingStructTypeFor("DoesNotExist");
	}

	@Test
	public void failsOnProhibitedAttribute() {
		assertFailureCreatingStructTypeFor("ProhibitedAttribute");
	}

	@Test
	public void abstractElementsYieldNoStructType() {
		assertFailureCreatingStructTypeFor("AbstractElement");
	}

	@Test
	public void failsOnElementsThatCanBeSubstituted() {
		assertFailureCreatingStructTypeFor("NameWithAlias");
	}

	@Test
	public void butAcceptsElementsThatCanSubstituteAnother() {
		assertThat(analyzer.typeOf("Alias")).isEqualTo(STRING);
	}

	@Test
	public void failsOnAnyAttribute() {
		assertFailureCreatingStructTypeFor("WithAnyAttribute");
	}

	/*
	 * Scalar type variations.
	 */

	@Test
	public void simpleTypesMayNotBeAList() {
		assertFailureCreatingStructTypeFor("list");
	}

	@Test
	public void simpleTypesMayNotBeAUnion() {
		assertFailureCreatingStructTypeFor("union");
	}

	@Test
	public void simpleTypeRestrictionsMayContainASimpleType() {
		assertThat(analyzer.typeOf("nestedSimpleType")).isEqualTo(DecimalType.INTEGER_TYPE);
	}

	@Test
	public void uriValuesAreAllowed() {
		assertThat(analyzer.typeOf("uri")).isEqualTo(STRING);
	}

	@Test
	public void booleanValuesAreAllowed() {
		assertThat(analyzer.typeOf("boolean")).isEqualTo(BOOLEAN);
	}

	@Test
	public void intValuesAreAllowed() {
		assertThat(analyzer.typeOf("int")).isEqualTo(DecimalType.INTEGER_TYPE);
	}

	@Test
	public void longValuesAreAllowed() {
		assertThat(analyzer.typeOf("long")).isEqualTo(DecimalType.LONG_TYPE);
	}

	@Test
	public void floatValuesAreAllowed() {
		assertThat(analyzer.typeOf("float")).isEqualTo(FLOAT);
	}

	@Test
	public void doubleValuesAreAllowed() {
		assertThat(analyzer.typeOf("double")).isEqualTo(DOUBLE);
	}

	@Test
	public void stringValuesAreAllowed() {
		assertThat(analyzer.typeOf("string")).isEqualTo(STRING);
	}

	@Test
	public void timestampValuesAreAllowed() {
		assertThat(analyzer.typeOf("timestamp")).isEqualTo(DATETIME);
	}

	@Test
	public void dateValuesAreAllowed() {
		assertThat(analyzer.typeOf("date")).isEqualTo(DATE);
	}

	@Test
	public void timeValuesAreAllowed() {
		assertThat(analyzer.typeOf("time")).isEqualTo(TIME);
	}

	@Test
	public void unknownTypesAreNotAllowed() {
		// Note: the element exists, but it uses an unsupported type
		assertFailureCreatingStructTypeFor("unsupportedSimpleType");
	}

	@Test
	public void unconstrainedDecimalAttributesAreNotAllowed() {
		assertFailureCreatingStructTypeFor("unconstrainedDecimalAttribute");
	}

	@Test
	public void unconstrainedIntegerAttributesAreCoercedToLong() {
		Type type = analyzer.typeOf("unconstrainedIntegerAttribute");
		assertThat(type).isEqualTo(struct("namespace.unconstrainedIntegerAttribute").withFields(
				optional("value", DecimalType.LONG_TYPE)
		));
	}

	@Test
	public void decimalsMustHaveConstraints() {
		assertFailureCreatingStructTypeFor("unconstrainedDecimal");
	}

	@Test
	public void decimalsCanBeInfinite() {
		Type type = analyzer.typeOf("unboundedDecimal");
		assertThat(type).is(decimalWithPrecisionAndScale(Integer.MAX_VALUE, 6));
	}

	@Test
	public void decimalsCanHavePrecisionAndScale() {
		Type type = analyzer.typeOf("decimalBoundedByPrecision");
		assertThat(type).is(decimalWithPrecisionAndScale(4, 2));
	}

	@Test
	public void decimalsCanHaveBoundsAndScale() {
		Type type = analyzer.typeOf("decimalBoundedByLimits");
		assertThat(type).is(decimalWithPrecisionAndScale(9, 2));
	}

	@Test
	public void integersHaveNoFraction() {
		assertFailureCreatingStructTypeFor("integerWithFractionMakesNoSense");
	}

	@Test
	public void unconstrainedIntegersAreCoercedToLong() {
		Type type = analyzer.typeOf("coercedToLong");
		assertThat(type).isEqualTo(DecimalType.LONG_TYPE);
	}

	@Test
	public void integersWithFewDigitsAreCoercedToInteger() {
		Type type = analyzer.typeOf("integerWithFewDigits");
		assertThat(type).isEqualTo(DecimalType.INTEGER_TYPE);
	}

	@Test
	public void integersWithSmallExclusiveBoundsAreCoercedToInteger() {
		Type type = analyzer.typeOf("integerWithSmallExclusiveBounds");
		assertThat(type).isEqualTo(DecimalType.INTEGER_TYPE);
	}

	@Test
	public void integersWithSmallInclusiveBoundsAreCoercedToInteger() {
		Type type = analyzer.typeOf("integerWithSmallInclusiveBounds");
		assertThat(type).isEqualTo(DecimalType.INTEGER_TYPE);
	}

	@Test
	public void integersWithMediumDigitsAreCoercedToLong() {
		Type type = analyzer.typeOf("integerWithMediumDigits");
		assertThat(type).isEqualTo(DecimalType.LONG_TYPE);
	}

	@Test
	public void integersWithMediumBoundsAreCoercedToLong() {
		Type type = analyzer.typeOf("integerWithMediumBounds");
		assertThat(type).isEqualTo(DecimalType.LONG_TYPE);
	}

	@Test
	public void integersWithManyDigitsAreSupported() {
		Type type = analyzer.typeOf("integerWithManyDigits");
		assertThat(type).is(decimalWithPrecisionAndScale(20, 0));
	}

	@Test
	public void integersWithLargeBoundsAreSupported() {
		Type type = analyzer.typeOf("integerWithLargeBounds");
		assertThat(type).is(decimalWithPrecisionAndScale(19, 0));
	}

	@Test
	public void enumerationsAreSupported() {
		analyzer.mapTargetNamespace("");
		Type type = analyzer.typeOf("enumeration");
		assertThat(type).isEqualTo(enumType("enumeration", null, List.of("NONE", "BICYCLE", "BUS", "TRAIN", "CAR")));
	}

	@Test
	public void binaryDataIsSupported() {
		assertThat(analyzer.typeOf("hexEncodedBinary")).isEqualTo(BINARY_HEX);
		assertThat(analyzer.typeOf("base64EncodedBinary")).isEqualTo(BINARY_BASE64);
	}

	/*
	 * Test stuff that should not occur (i.e., protect against famous last words).
	 */

	@Test
	public void coverMethodThatCannotBeCalled() {
		// As substitution groups are not supported, onEnterSubstitutionGroup(...) always throws and onExitSubstitutionGroup(...) is never called.
		// We call it here to ensure that the statement "< 100% coverage means the code may fail unpredictably" is still true.
		new StructuralSchemaVisitor<>((StructureBuilder<?, ?>) null, null, Integer.MAX_VALUE).onExitSubstitutionGroup(null);
	}

	/*
	 * Test record toString()'s used when debugging.
	 */

	@Test
	public void validateVisitorContextAsText() {
		// Note that the given result is not nonsense: a field with a simpleType has a type that resulted in the simpleType
		assertThat(new StructuralSchemaVisitor.VisitorContext<>(
				new FieldData("field", null, REQUIRED, STRING, "abc"),
				new TypeData("type", null, false),
				"whatever").toString()).isEqualTo("field: string=abc is a type with whatever");
	}

	/*
	 * Utilities
	 */

	private void assertFailureCreatingStructTypeFor(String rootElement) {
		assertThatThrownBy(() -> {
			// We're not interested in the result, but the isNull assertion makes debugging a failed test much easier.
			assertThat(analyzer.typeOf(rootElement)).isNull();
		}).isInstanceOf(IllegalArgumentException.class);
	}

	private Condition<? super Type> decimalWithPrecisionAndScale(int precision, int scale) {
		return new Condition<Object>(object -> {
			try {
				DecimalType decimalType = (DecimalType) object;
				return decimalType.precision() == precision && decimalType.scale() == scale;
			} catch (ClassCastException | NullPointerException | IllegalArgumentException ignored) {
				return false;
			}
		}, "a decimal(%d,%d)", precision, scale);
	}

	private static class Indent {
		private final String baseIndent;
		private String currentIndent;

		public Indent(String indent) {
			baseIndent = indent;
			currentIndent = "";
		}

		public String inc() {
			String oldIndent = currentIndent;
			currentIndent += baseIndent;
			return oldIndent;
		}

		public String dec() {
			currentIndent = currentIndent.substring(2);
			return currentIndent;
		}

		@Override
		public String toString() {
			return currentIndent;
		}
	}
}
