package opwvhk.avro;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opwvhk.avro.xsd.FieldData;
import opwvhk.avro.xsd.FixedType;
import opwvhk.avro.xsd.ScalarType;
import opwvhk.avro.xsd.StructureBuilder;
import opwvhk.avro.xsd.TypeData;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
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
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static opwvhk.avro.xsd.Cardinality.MULTIPLE;
import static opwvhk.avro.xsd.Cardinality.OPTIONAL;
import static opwvhk.avro.xsd.Cardinality.REQUIRED;
import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class XsdAnalyzerTest {
	private static final String PART_IDENTIFIER = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
	private static final Pattern START_OF_NAMED_SCHEMA_IN_IDL = Pattern.compile(
			"\\b(?:enum|record|fixed)\\s+(" + PART_IDENTIFIER + "(?:\\." + PART_IDENTIFIER + ")*)\\s*[{(]");
	private static XsdAnalyzer analyzer;
	public static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
	public static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
	public static final Schema STRING_ARRAY_SCHEMA = Schema.createArray(STRING_SCHEMA);
	public static final Schema OPTIONAL_INT_SCHEMA = Schema.createUnion(Schema.create(Schema.Type.NULL), INT_SCHEMA);
	public static final Schema OPTIONAL_STRING_SCHEMA = Schema.createUnion(Schema.create(Schema.Type.NULL), STRING_SCHEMA);
	public static final Schema OPTIONAL_STRING_SCHEMA2 = Schema.createUnion(STRING_SCHEMA, Schema.create(Schema.Type.NULL));

	@BeforeClass
	public static void beforeClass()
			throws Exception {
		URL schemaUrl = XsdAnalyzerTest.class.getResource("/testCases.xsd");
		analyzer = new XsdAnalyzer(Objects.requireNonNull(schemaUrl));
		analyzer.mapTargetNamespace("namespace");
	}

	@Test
	public void checkTestNamespaces() {
		assertThat(analyzer.availableNamespaces()).containsExactly("https://www.schiphol.nl/avro-tools/tests");
	}

	@Test
	public void checkXmlNamespaceIsCorrectlySpecified()
			throws IOException {
		assertThatThrownBy(() -> analyzer.createAvroSchema(new QName("unknown", "unknown"))).isInstanceOf(NullPointerException.class);

		URL schemaUrl = XsdAnalyzerTest.class.getResource("/testCases.xsd");
		XsdAnalyzer extraAnalyzer = new XsdAnalyzer(Objects.requireNonNull(schemaUrl));
		extraAnalyzer.mapNamespace("https://www.schiphol.nl/avro-tools/tests", "namespace");
		extraAnalyzer.mapNamespace("ExtraNameSpace", "extra");
		assertThatThrownBy(() -> extraAnalyzer.createAvroSchema("unknown")).isInstanceOf(IllegalArgumentException.class);
	}

	/*
	 * Happy flow variations.
	 */

	@Test
	public void groupedStructuresAreHandledAndDocumentedCorrectly() {
		Schema schema = analyzer.createAvroSchema(new QName("https://www.schiphol.nl/avro-tools/tests", "GroupStructures"));
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("GroupStructures",
				"Record documentation is taken from the type if possible, from the element otherwise", "namespace", false, List.of(
						new Schema.Field("comment", OPTIONAL_STRING_SCHEMA, "A comment describing the group; can be placed before or after it in the XML.",
								NULL_DEFAULT_VALUE),
						new Schema.Field("group", Schema.createRecord("Group", "This documents both the field and the record", "namespace", false, List.of(
								new Schema.Field("keep", Schema.createRecord("Keep", null, "namespace", false, singletonList(
										new Schema.Field("value", STRING_SCHEMA)
								))),
								new Schema.Field("one", STRING_ARRAY_SCHEMA, null, emptyList()),
								new Schema.Field("other", STRING_ARRAY_SCHEMA, null, emptyList())
						)), "This documents both the field and the record"))));
	}

	@Test
	public void attributesAreSupported() {
		Schema schema = analyzer.createAvroSchema("AttributesAndAnnotationWithoutDocs");

		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("AttributesAndAnnotationWithoutDocs", null, "namespace", false, List.of(
				new Schema.Field("id", INT_SCHEMA),
				new Schema.Field("something", OPTIONAL_INT_SCHEMA, null, NULL_DEFAULT_VALUE)
		)));
	}

	@Test
	public void attributesWithSimpleContentAreSupported() {
		Schema schema = analyzer.createAvroSchema("ExtensionInSimpleContent");

		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("ExtensionInSimpleContent", null, "namespace", false, List.of(
				new Schema.Field("value", STRING_SCHEMA),
				new Schema.Field("version", OPTIONAL_STRING_SCHEMA, null, NULL_DEFAULT_VALUE)
		)));
	}

	@Test
	public void attributesWithComplexContentAreSupported() {
		Schema schema = analyzer.createAvroSchema("ExtensionInMixedComplexContent");

		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("ExtensionInMixedComplexContent", null, "namespace", false, List.of(
				new Schema.Field("value", STRING_SCHEMA),
				new Schema.Field("version", OPTIONAL_STRING_SCHEMA, null, NULL_DEFAULT_VALUE)
		)));
	}

	@Test
	public void repeatedNestedValuesBecomeArrays() {
		Schema schema = analyzer.createAvroSchema("RepeatedNestedRecordWithOptionalField");

		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("RepeatedNestedRecordWithOptionalField", null, "namespace", false, List.of(
				new Schema.Field("array", Schema.createArray(Schema.createRecord("Array", null, "namespace", false, List.of(
						new Schema.Field("one", STRING_SCHEMA),
						new Schema.Field("two", OPTIONAL_STRING_SCHEMA, null, NULL_DEFAULT_VALUE)
				))), null, emptyList()),
				new Schema.Field("id", STRING_SCHEMA)
		)));
	}

	@Test
	public void repeatedSequencedElementsBecomeArrays() {
		Schema schema = analyzer.createAvroSchema("RepeatedSequence");

		Schema namedSchema = Schema.createRecord("Named", null, "namespace", false, List.of(
				new Schema.Field("description", STRING_SCHEMA),
				new Schema.Field("name", STRING_SCHEMA)
		));
		Schema namedArraySchema = Schema.createArray(namedSchema);
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("RepeatedSequence", null, "namespace", false, List.of(
				new Schema.Field("array1", namedArraySchema, null, emptyList()),
				new Schema.Field("array2", namedArraySchema, null, emptyList())
		)));
	}

	@Test
	public void repeatedChoiceElementsBecomeArrays() {
		Schema schema = analyzer.createAvroSchema("RepeatedChoice");

		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("RepeatedChoice", null, "namespace", false, List.of(
				new Schema.Field("value", STRING_ARRAY_SCHEMA, null, emptyList())
		)));
	}

	@Test
	public void optionalAllMakesElementsNullable() {
		Schema schema = analyzer.createAvroSchema("OptionalAll");

		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("OptionalAll", null, "namespace", false, List.of(
				new Schema.Field("value1", OPTIONAL_STRING_SCHEMA, null, NULL_DEFAULT_VALUE),
				new Schema.Field("value2", OPTIONAL_STRING_SCHEMA, null, NULL_DEFAULT_VALUE)
		)));
	}

	@Test
	public void allowsRestrictionInSimpleContent() {
		Schema schema = unwrap(analyzer.createAvroSchema("RestrictionInSimpleContent"));
		assertThat(schema).isEqualTo(Schema.create(Schema.Type.STRING));
	}

	@Test
	public void allowsRestrictionInComplexContent() {
		Schema schema = unwrap(analyzer.createAvroSchema("RestrictionInComplexContent"));
		assertThat(schema).isEqualTo(Schema.create(Schema.Type.STRING));
	}

	@Test
	public void allowsExtensionWithElements() {
		Schema schema = analyzer.createAvroSchema("ExtensionWithElements");
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("ExtensionWithElements", null, "namespace", false, List.of(
				new Schema.Field("description", STRING_SCHEMA),
				new Schema.Field("field", STRING_SCHEMA),
				new Schema.Field("name", STRING_SCHEMA)
		)));
	}

	@Test
	public void allowsExtensionOfComplexType() {
		Schema schema = analyzer.createAvroSchema("ExtensionOfComplexType");
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(Schema.createRecord("ExtensionOfComplexType", null, "namespace", false, List.of(
				new Schema.Field("value", STRING_SCHEMA),
				new Schema.Field("version", OPTIONAL_STRING_SCHEMA, null, NULL_DEFAULT_VALUE)
		)));
	}

	@Test
	public void arbitraryXmlDataIsReadAsString() {
		Schema expected = Schema.createRecord("ArbitraryContent", null, "namespace", false, List.of(
				new Schema.Field("source", STRING_SCHEMA),
				new Schema.Field("value", STRING_SCHEMA, "The entire element content, unparsed.")
		));
		assertThat(analyzer.createAvroSchema("ArbitraryContent")).isEqualTo(expected);
	}

	@Test
	public void mixedComplexTypesAreCoercedToString() {
		Schema expected = Schema.createRecord("MixedComplexType", null, "namespace", false, List.of(
				new Schema.Field("source", STRING_SCHEMA),
				new Schema.Field("payload", STRING_SCHEMA, "The entire element content, unparsed.")
		));

		assertThat(analyzer.createAvroSchema("MixedComplexType")).isEqualTo(expected);
	}

	@Test
	public void mixedComplexContentTreatedAsNormal() {
		Schema schema = analyzer.createAvroSchema("MixedExtensionWithElements");
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(
				Schema.createRecord("MixedExtensionWithElements", "Note that the complexContent being mixed does not affect the outcome!", "namespace", false,
						List.of(
								new Schema.Field("description", STRING_SCHEMA),
								new Schema.Field("field", STRING_SCHEMA),
								new Schema.Field("name", STRING_SCHEMA)
						)));
	}

	@Test
	public void defaultValuesAreAddedIfPossible() {
		// Note: because we have a default value, the field becomes required (as there's always a value).
		// Reason: nil and absent values are treated equally, and mean "there's no value in the XML, so use the default"
		Schema expected = parseNamedSchema("namespace", """
				record DefaultValuesForFields {
					string req = "ghi";
					string? opt = "jkl";
					string required = "abc";
					optional? optional = null;
					defaultToNull? defaultToNull = null;
					array<string> `array` = [];
				}
				record optional {
					string optimizedAway = "def";
				}
				record defaultToNull {
					string? optimizedAway = null;
				}
				""");
		assertThat(analyzer.createAvroSchema("DefaultValuesForFields")).isEqualTo(expected);
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
		}, ScalarType.userRecognizedTypes());
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
				    onEnterElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}wrappedNumberArray
				      onEndAttributes (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}wrappedNumberArray
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
				    onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}wrappedNumberArray
				  onExitSequenceGroup (occurs 1-1) sequence
				onExitElement (occurs 1-1) {https://www.schiphol.nl/avro-tools/tests}Recursive
				""");
	}

	@Test
	public void recursionYieldsSchemasAsWell() {
		Schema recursiveComplexType = Schema.createRecord("RecursiveComplexType", null, "namespace", false);
		recursiveComplexType.setFields(List.of(
				new Schema.Field("level", INT_SCHEMA),
				new Schema.Field("rabbitHole", recursiveComplexType)
		));
		Schema recursiveType = Schema.createRecord("Recursive", null, "namespace", false);
		Schema numberWithLength = Schema.createRecord("Array", null, "namespace", false, List.of(
				new Schema.Field("length", OPTIONAL_INT_SCHEMA, null, NULL_DEFAULT_VALUE),
				new Schema.Field("numberElement", INT_SCHEMA)
		));
		recursiveType.setFields(List.of(
				new Schema.Field("holeInTheGround", recursiveComplexType),
				new Schema.Field("recursive", recursiveType),
				new Schema.Field("wrappedNumberArray", Schema.createArray(numberWithLength), null, emptyList()),
				new Schema.Field("wrappedStringArray", STRING_ARRAY_SCHEMA, null, emptyList())
		));

		Schema schema = analyzer.createAvroSchema("Recursive");
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(recursiveType);
	}

	@Test
	public void sortingFieldsCanHelpWihTesting() {
		Schema schema = AvroSchemaUtils.sortFields(analyzer.createAvroSchema("SortableFields"));

		assertThat(schema).isEqualTo(Schema.createRecord("SortableFields", null, "namespace", false, List.of(
				new Schema.Field("a", STRING_SCHEMA),
				new Schema.Field("b", STRING_SCHEMA)
		)));
	}

	@Test
	public void snakeCaseCanBeConvertedToCamelCase() {
		assertThat(Utils.snakeToUpperCamelCase("MVP_description")).isEqualTo("MVPDescription");
		assertThat(Utils.snakeToUpperCamelCase("URL_list")).isEqualTo("URLList");
		assertThat(Utils.snakeToUpperCamelCase("URLs")).isEqualTo("URLs");
		assertThat(Utils.snakeToUpperCamelCase("simple_name")).isEqualTo("SimpleName");

		assertThat(Utils.snakeToLowerCamelCase("MVP_description")).isEqualTo("mvpDescription");
		assertThat(Utils.snakeToLowerCamelCase("URL_list")).isEqualTo("urlList");
		assertThat(Utils.snakeToLowerCamelCase("URLs")).isEqualTo("urls");
		assertThat(Utils.snakeToLowerCamelCase("simple_name")).isEqualTo("simpleName");
	}

	/*
	 * Edge cases of internal methods, etc.
	 */

	@Test
	public void validateClassNameUniqueness() {
		Schema expected = parseNamedSchema("namespace", """
				record ClassNamesEdgeCases {
					TypeName PlainType;
					TypeName2 TypeName;
					ClassNamesEdgeCases2 ClassNamesEdgeCases;
					Normal2 Normal;
				}
				record TypeName {
					Normal Normal;
				}
				record Normal {
					string field;
				}
				record TypeName2 {
					string name;
				}
				record ClassNamesEdgeCases2 {
					string description;
				}
				record Normal2 {
					string field;
				}
				""");
		assertThat(analyzer.createAvroSchema("ClassNamesEdgeCases")).isEqualTo(expected);
	}

	/*
	 * Unsupported structure variations.
	 */

	@Test
	public void failsIfElementNotFound() {
		assertFailureCreatingSchemaFor("DoesNotExist");
	}

	@Test
	public void failsOnProhibitedAttribute() {
		assertFailureCreatingSchemaFor("ProhibitedAttribute");
	}

	@Test
	public void abstractElementsYieldNoSchema() {
		assertFailureCreatingSchemaFor("AbstractElement");
	}

	@Test
	public void failsOnElementsThatCanBeSubstituted() {
		assertFailureCreatingSchemaFor("NameWithAlias");
	}

	@Test
	public void butAcceptsElementsThatCanSubstituteAnother() {
		assertThat(unwrap(analyzer.createAvroSchema("Alias"))).isEqualTo(STRING_SCHEMA);
	}

	@Test
	public void failsOnAnyAttribute() {
		assertFailureCreatingSchemaFor("WithAnyAttribute");
	}

	/*
	 * Scalar type variations.
	 */

	@Test
	public void simpleTypesMayNotBeAList() {
		assertFailureCreatingSchemaFor("list");
	}

	@Test
	public void simpleTypesMayNotBeAUnion() {
		assertFailureCreatingSchemaFor("union");
	}

	@Test
	public void simpleTypeRestrictionsMayContainASimpleType() {
		assertThat(unwrap(analyzer.createAvroSchema("nestedSimpleType")).getType()).isEqualTo(Schema.Type.INT);
	}

	@Test
	public void uriValuesAreAllowed() {
		assertThat(unwrap(analyzer.createAvroSchema("uri")).getType()).isEqualTo(Schema.Type.STRING);
	}

	@Test
	public void booleanValuesAreAllowed() {
		assertThat(unwrap(analyzer.createAvroSchema("boolean")).getType()).isEqualTo(Schema.Type.BOOLEAN);
	}

	@Test
	public void intValuesAreAllowed() {
		assertThat(unwrap(analyzer.createAvroSchema("int")).getType()).isEqualTo(Schema.Type.INT);
	}

	@Test
	public void longValuesAreAllowed() {
		assertThat(unwrap(analyzer.createAvroSchema("long")).getType()).isEqualTo(Schema.Type.LONG);
	}

	@Test
	public void floatValuesAreAllowed() {
		assertThat(unwrap(analyzer.createAvroSchema("float")).getType()).isEqualTo(Schema.Type.FLOAT);
	}

	@Test
	public void doubleValuesAreAllowed() {
		assertThat(unwrap(analyzer.createAvroSchema("double")).getType()).isEqualTo(Schema.Type.DOUBLE);
	}

	@Test
	public void stringValuesAreAllowed() {
		assertThat(unwrap(unwrap(analyzer.createAvroSchema("string"))).getType()).isEqualTo(Schema.Type.STRING);
	}

	@Test
	public void timestampValuesAreAllowed() {
		Schema avroSchema = unwrap(analyzer.createAvroSchema("timestamp"));
		assertThat(avroSchema.getType()).isEqualTo(Schema.Type.LONG);
		assertThat(avroSchema.getLogicalType().getName()).isEqualTo("timestamp-millis");
	}

	@Test
	public void dateValuesAreAllowed() {
		Schema avroSchema = unwrap(analyzer.createAvroSchema("date"));
		assertThat(avroSchema.getType()).isEqualTo(Schema.Type.INT);
		assertThat(avroSchema.getLogicalType().getName()).isEqualTo("date");
	}

	@Test
	public void timeValuesAreAllowed() {
		Schema avroSchema = unwrap(analyzer.createAvroSchema("time"));
		assertThat(avroSchema.getType()).isEqualTo(Schema.Type.INT);
		assertThat(avroSchema.getLogicalType().getName()).isEqualTo("time-millis");
	}

	@Test
	public void unknownTypesAreNotAllowed() {
		// Note: the element exists, but it uses an unsupported type
		assertFailureCreatingSchemaFor("unsupportedSimpleType");
	}

	@Test
	public void unconstrainedDecimalAttributesAreNotAllowed() {
		assertFailureCreatingSchemaFor("unconstrainedDecimalAttribute");
	}

	@Test
	public void unconstrainedIntegerAttributesAreCoercedToLong() {
		Schema avroSchema = unwrap(analyzer.createAvroSchema("unconstrainedIntegerAttribute"));
		assertThat(avroSchema.getType()).isEqualTo(Schema.Type.UNION);
		assertThat(avroSchema.getTypes()).containsExactly(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
	}

	@Test
	public void decimalsMustHaveConstraints() {
		assertFailureCreatingSchemaFor("unconstrainedDecimal");
	}

	@Test
	public void decimalsCanBeInfinite() {
		assertThat(unwrap(analyzer.createAvroSchema("unboundedDecimal"))).is(decimalWithPrecisionAndScale(Integer.MAX_VALUE, 6));
	}

	@Test
	public void decimalsCanHavePrecisionAndScale() {
		assertThat(unwrap(analyzer.createAvroSchema("decimalBoundedByPrecision"))).is(decimalWithPrecisionAndScale(4, 2));
	}

	@Test
	public void decimalsCanHaveBoundsAndScale() {
		assertThat(unwrap(analyzer.createAvroSchema("decimalBoundedByLimits"))).is(decimalWithPrecisionAndScale(9, 2));
	}

	@Test
	public void integersHaveNoFraction() {
		assertFailureCreatingSchemaFor("integerWithFractionMakesNoSense");
	}

	@Test
	public void unconstrainedIntegersAreCoercedToLong() {
		assertThat(unwrap(analyzer.createAvroSchema("coercedToLong")).getType()).isEqualTo(Schema.Type.LONG);
	}

	@Test
	public void integersWithFewDigitsAreCoercedToInteger() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithFewDigits")).getType()).isEqualTo(Schema.Type.INT);
	}

	@Test
	public void integersWithSmallExclusiveBoundsAreCoercedToInteger() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithSmallExclusiveBounds")).getType()).isEqualTo(Schema.Type.INT);
	}

	@Test
	public void integersWithSmallInclusiveBoundsAreCoercedToInteger() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithSmallInclusiveBounds")).getType()).isEqualTo(Schema.Type.INT);
	}

	@Test
	public void integersWithMediumDigitsAreCoercedToLong() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithMediumDigits")).getType()).isEqualTo(Schema.Type.LONG);
	}

	@Test
	public void integersWithMediumBoundsAreCoercedToLong() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithMediumBounds")).getType()).isEqualTo(Schema.Type.LONG);
	}

	@Test
	public void integersWithManyDigitsAreSupported() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithManyDigits"))).is(decimalWithPrecisionAndScale(20, 0));
	}

	@Test
	public void integersWithLargeBoundsAreSupported() {
		assertThat(unwrap(analyzer.createAvroSchema("integerWithLargeBounds"))).is(decimalWithPrecisionAndScale(19, 0));
	}

	@Test
	public void enumerationsAreSupported() {
		assertThat(unwrap(analyzer.createAvroSchema("enumeration"))).isEqualTo(
				Schema.createEnum("Enumeration", null, "namespace", List.of(
						"NONE", "BICYCLE", "BUS", "TRAIN", "CAR"
				)));
	}

	@Test
	public void binaryDataIsSupported() {
		assertThat(unwrap(analyzer.createAvroSchema("hexEncodedBinary"))).isEqualTo(Schema.create(Schema.Type.BYTES));
		assertThat(unwrap(analyzer.createAvroSchema("base64EncodedBinary"))).isEqualTo(Schema.create(Schema.Type.BYTES));
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
	 * Test parsing default values.
	 */

	/*
	TODO: refactor
	@Test
	public void testDefaultValues() {
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.STRING), "ABC")).isEqualTo("ABC");
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.createEnum("enum", null, null, emptyList()), "ABC")).isEqualTo("ABC");
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.BYTES), "ABC")).isEqualTo("ABC");

		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.INT), "42")).isEqualTo(42);
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.LONG), "42")).isEqualTo(42L);
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.BOOLEAN), "false")).isEqualTo(false);
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.DOUBLE), "4.2")).isEqualTo(4.2);
		assertThat(XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.FLOAT), "4.2")).isEqualTo(4.2f);

		assertThatThrownBy(() -> XsdAnalyzer.parseSimpleValue(Schema.create(Schema.Type.NULL), "")).isInstanceOf(IllegalArgumentException.class);

		assertThat(XsdAnalyzer.parseSimpleValue(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), "1970-02-03"))
				.isEqualTo(33);
		assertThat(XsdAnalyzer.parseSimpleValue(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)), "01:02:03"))
				.isEqualTo(3723000);
		assertThat(XsdAnalyzer.parseSimpleValue(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)), "1970-02-03T01:02:03Z"))
				.isEqualTo(2854923000L);

		assertThatThrownBy(() -> XsdAnalyzer.parseSimpleValue(LogicalTypes.decimal(5).addToSchema(Schema.create(Schema.Type.BYTES)), ""))
				.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> XsdAnalyzer.parseSimpleValue(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.BYTES)), ""))
				.isInstanceOf(IllegalArgumentException.class);
	}
	*/

	/*
	 * Test record toString()'s used when debugging.
	 */

	@Test
	public void validateFieldDataAsText() {
		assertThat(new FieldData("field", null, REQUIRED, null, "abc").toString())
				.isEqualTo("field");
		assertThat(new FieldData("field", "documented", OPTIONAL, FixedType.STRING, "abc").toString())
				.isEqualTo("field?: string=abc (documented)");
		assertThat(new FieldData("field", "much more documentation", MULTIPLE, FixedType.STRING, null).toString())
				.isEqualTo("field[]: string (much more…)");
	}

	@Test
	public void validateTypeDataAsText() {
		assertThat(new TypeData(null, null, false).toString()).isEqualTo("anonymous");
		assertThat(new TypeData("type", null, true).toString()).isEqualTo("type (mixed)");
		assertThat(new TypeData("type", "something", true).toString()).isEqualTo("type (mixed; something)");
		assertThat(new TypeData("type", "much more text", false).toString()).isEqualTo("type (much more…)");
	}

	@Test
	public void validateVisitorContextAsText() {
		// Note that the given result is not nonsense: a field with a simpleType has a type that resulted in the simpleType
		assertThat(new StructuralSchemaVisitor.VisitorContext<>(
				new FieldData("field", null, REQUIRED, FixedType.STRING, "abc"),
				new TypeData("type", null, false),
				"whatever").toString()).isEqualTo("field: string=abc is a type with whatever");
	}

	/*
	 * Utilities
	 */

	private void assertFailureCreatingSchemaFor(String rootElement) {
		assertThatThrownBy(() -> {
			Schema schema = analyzer.createAvroSchema(rootElement);
			assertThat(schema).isNull();
		}).isInstanceOf(IllegalArgumentException.class);
	}

	private Condition<? super Schema> decimalWithPrecisionAndScale(int precision, int scale) {
		return new Condition<Object>(object -> {
			try {
				Schema schema = (Schema) object;
				LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
				decimalType.validate(schema);
				return decimalType.getPrecision() == precision && decimalType.getScale() == scale;
			} catch (ClassCastException | NullPointerException | IllegalArgumentException ignored) {
				return false;
			}
		}, "a decimal(%d,%d)", precision, scale);
	}

	private static Schema parseNamedSchema(String namespace, String idlSchema) {
		try {
			Matcher matcher = START_OF_NAMED_SCHEMA_IN_IDL.matcher(idlSchema);
			if (!matcher.find()) {
				throw new ParseException("Must be a schema in IDL format");
			}
			String recordName = matcher.group(1);
			Idl idl = new Idl(new StringReader("@namespace(\"" + namespace + "\")protocol Foo {\n" + idlSchema + "\n}"));
			Protocol protocol = idl.CompilationUnit();
			return protocol.getType(namespace + "." + recordName);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Not a named schema", e);
		}
	}

	private Schema unwrap(Schema schema) {
		if (schema.getType() == Schema.Type.RECORD) {
			List<Schema.Field> fields = schema.getFields();
			if (fields.size() == 1) {
				return fields.get(0).schema();
			}
		}
		return schema;
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
