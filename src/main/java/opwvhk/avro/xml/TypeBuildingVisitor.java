package opwvhk.avro.xml;

import opwvhk.avro.xml.datamodel.Cardinality;
import opwvhk.avro.xml.datamodel.DecimalType;
import opwvhk.avro.xml.datamodel.EnumType;
import opwvhk.avro.xml.datamodel.FixedType;
import opwvhk.avro.xml.datamodel.ScalarType;
import opwvhk.avro.xml.datamodel.Type;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAnnotated;
import org.apache.ws.commons.schema.XmlSchemaAnnotation;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaDocumentation;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaType;
import org.apache.ws.commons.schema.utils.XmlSchemaNamed;
import org.apache.ws.commons.schema.walker.XmlSchemaAttrInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaRestriction;
import org.apache.ws.commons.schema.walker.XmlSchemaTypeInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.w3c.dom.Node;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.namespace.QName;

import static java.lang.String.format;
import static java.math.BigInteger.ONE;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.ws.commons.schema.constants.Constants.XSD_ANYURI;
import static org.apache.ws.commons.schema.constants.Constants.XSD_BASE64;
import static org.apache.ws.commons.schema.constants.Constants.XSD_BOOLEAN;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DATE;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DATETIME;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DECIMAL;
import static org.apache.ws.commons.schema.constants.Constants.XSD_DOUBLE;
import static org.apache.ws.commons.schema.constants.Constants.XSD_FLOAT;
import static org.apache.ws.commons.schema.constants.Constants.XSD_HEXBIN;
import static org.apache.ws.commons.schema.constants.Constants.XSD_INT;
import static org.apache.ws.commons.schema.constants.Constants.XSD_LONG;
import static org.apache.ws.commons.schema.constants.Constants.XSD_STRING;
import static org.apache.ws.commons.schema.constants.Constants.XSD_TIME;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.DIGITS_FRACTION;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.DIGITS_TOTAL;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.ENUMERATION;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.EXCLUSIVE_MAX;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.EXCLUSIVE_MIN;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.INCLUSIVE_MAX;
import static org.apache.ws.commons.schema.walker.XmlSchemaRestriction.Type.INCLUSIVE_MIN;

class TypeBuildingVisitor implements XmlSchemaVisitor {
	private final TypeStructureBuilder structureBuilder;
	private final Function<String, String> xmlToTypeNamespace;
	private final int maxDuplicateClasses;
	private final Deque<Cardinality> cardinalityStack;
	private final Deque<VisitorContext> contextStack;
	private final IdentityHashMap<XmlSchemaType, String> generatedClassNames;
	private Type result;

	TypeBuildingVisitor(TypeStructureBuilder structureBuilder, Function<String, String> xmlToTypeNamespace,
	                    int maxDuplicateClasses) {
		this.structureBuilder = structureBuilder;
		this.xmlToTypeNamespace = xmlToTypeNamespace;
		this.maxDuplicateClasses = maxDuplicateClasses;
		cardinalityStack = new ArrayDeque<>();
		cardinalityStack.push(Cardinality.DEFAULT_VALUE);
		contextStack = new ArrayDeque<>();
		generatedClassNames = new IdentityHashMap<>();
	}

	public Type result() {
		return result;
	}

	private static String extractDocumentation(XmlSchemaAnnotated annotated) {
		final XmlSchemaAnnotation annotation = annotated.getAnnotation();
		if (annotation == null) {
			return null;
		}
		String documentation = annotation.getItems().stream().filter(XmlSchemaDocumentation.class::isInstance).map(XmlSchemaDocumentation.class::cast).map(
				XmlSchemaDocumentation::getMarkup).flatMap(nodeList -> {
			final Stream.Builder<Node> builder = Stream.builder();
			for (int i = 0; i < nodeList.getLength(); i++) {
				builder.add(nodeList.item(i));
			}
			return builder.build();
		}).map(Node::getTextContent).collect(Collectors.joining("\n"));
		return documentation.isBlank() ? null : documentation.translateEscapes().strip().stripIndent();
	}

	@Override
	public void onEnterElement(XmlSchemaElement element, XmlSchemaTypeInfo typeInfo, boolean previouslyVisited) {
		Cardinality elementCardinality = cardinalityStack.element().adjustFor(Cardinality.of(element));

		TypeData typeData = typeData(element, element.getSchemaType());

		ScalarType scalarType = mapScalarType(typeData, typeInfo);
		String defaultValue = scalarType == null ? null : element.getDefaultValue();
		FieldData fieldData = new FieldData(element.getName(), extractDocumentation(element), elementCardinality, scalarType, defaultValue);

		cardinalityStack.push(Cardinality.DEFAULT_VALUE);
		contextStack.push(new VisitorContext(fieldData, typeData));
	}

	private TypeData typeData(XmlSchemaNamed element, XmlSchemaType schemaType) {
		// This works because anonymous schema types are unique
		String className = generatedClassNames.computeIfAbsent(schemaType, type -> className(element, type));

		String doc = extractDocumentation(schemaType);
		if (doc == null) {
			doc = extractDocumentation((XmlSchemaAnnotated) element);
		}
		return new TypeData(className, doc, schemaType.isMixed());
	}

	private String className(XmlSchemaNamed element, XmlSchemaType schemaType) {
		QName name = schemaType.getQName();
		boolean mustHaveSuffix;
		if (name == null) {
			name = element.getQName();
			XmlSchema xmlSchema = element.getParent();
			boolean existsTypeWithSameName = xmlSchema.getTypeByName(name) != null;
			boolean existsToplevelElementWithSameName = xmlSchema.getElementByName(name) != null;
			mustHaveSuffix = existsTypeWithSameName || !element.isTopLevel() && existsToplevelElementWithSameName;
		} else {
			mustHaveSuffix = false;
		}

		String xmlNamespace = name.getNamespaceURI();
		if (Constants.XML_SCHEMA_DEFINITION_NAMESPACES.contains(xmlNamespace)) {
			return null;
		}

		String typeNamespace = xmlToTypeNamespace.apply(xmlNamespace);
		String className;
		if (typeNamespace.isEmpty()) {
			className = name.getLocalPart();
		} else {
			className = typeNamespace + "." + name.getLocalPart();
		}

		// Note: IdentityHashMap also checks values by object identity instead of equality. We need the latter though.
		if (!mustHaveSuffix && generatedClassNames.values().stream().noneMatch(className::equals)) {
			return className;
		}
		int i = 2; // 1-based, but don't add the suffix 1 (also requires <= instead of < below)
		while (i <= maxDuplicateClasses) {
			String extraClassName = className + i++;
			if (generatedClassNames.values().stream().noneMatch(extraClassName::equals)) {
				return extraClassName;
			}
		}
		throw new IllegalStateException("Too many classes named %s; maximum sequence number (%d) reached.".formatted(className, maxDuplicateClasses));
	}

	private ScalarType mapScalarType(TypeData typeData, XmlSchemaTypeInfo typeInfo) {
		switch (typeInfo.getType()) {
			case COMPLEX -> {
				return null;
			}
			case ATOMIC -> {
				QName recognizedType = typeInfo.getUserRecognizedType();
				if (XSD_BOOLEAN.equals(recognizedType)) {
					return FixedType.BOOLEAN;
				} else if (XSD_FLOAT.equals(recognizedType)) {
					return FixedType.FLOAT;
				} else if (XSD_DOUBLE.equals(recognizedType)) {
					return FixedType.DOUBLE;
				} else if (XSD_DATE.equals(recognizedType)) {
					return FixedType.DATE;
				} else if (XSD_DATETIME.equals(recognizedType)) {
					return FixedType.DATETIME;
				} else if (XSD_TIME.equals(recognizedType)) {
					return FixedType.TIME;
				} else if (XSD_INT.equals(recognizedType)) {
					// XSD_BYTE and XSD_SHORT match this branch unless overridden.
					return DecimalType.INTEGER_TYPE;
				} else if (XSD_LONG.equals(recognizedType)) {
					return DecimalType.LONG_TYPE;
				} else if (XSD_DECIMAL.equals(recognizedType)) {
					// XSD_INT and XSD_LONG would match this branch if it weren't overridden via XsdAnalyzer.USER_RECOGNIZED_TYPES
					Optional<Integer> fractionDigitsRestriction = restriction(typeInfo, DIGITS_FRACTION, Integer::valueOf);
					if (fractionDigitsRestriction.isEmpty()) {
						// xs:decimal without scale -> use a double instead
						return FixedType.DOUBLE;
					}
					int fractionDigits = fractionDigitsRestriction.get();
					Optional<Integer> totalDigits = restriction(typeInfo, DIGITS_TOTAL, Integer::valueOf);
					UnaryOperator<BigDecimal> round = bd -> bd.setScale(fractionDigits, HALF_UP);
					UnaryOperator<BigDecimal> incULP = bd -> new BigDecimal(bd.unscaledValue().add(ONE), fractionDigits);
					UnaryOperator<BigDecimal> decULP = bd -> new BigDecimal(bd.unscaledValue().subtract(ONE), fractionDigits);
					Optional<BigDecimal> minInclusive = restriction(typeInfo, INCLUSIVE_MIN, BigDecimal::new).map(round);
					Optional<BigDecimal> minExclusive = restriction(typeInfo, EXCLUSIVE_MIN, BigDecimal::new).map(round).map(incULP);
					Optional<BigDecimal> maxInclusive = restriction(typeInfo, INCLUSIVE_MAX, BigDecimal::new).map(round);
					Optional<BigDecimal> maxExclusive = restriction(typeInfo, EXCLUSIVE_MAX, BigDecimal::new).map(round).map(decULP);
					Optional<BigDecimal> maxDigits = totalDigits.map(BigDecimal.TEN::pow).map(decULP);
					Optional<BigDecimal> minDigits = maxDigits.map(BigDecimal::negate);

					Optional<Integer> calculatedNumberOfDigits = Stream.concat(totalDigits.stream(),
									Stream.of(minInclusive, minExclusive, maxInclusive, maxExclusive).flatMap(Optional::stream).map(BigDecimal::precision))
							.max(Integer::compareTo);
					if (fractionDigits > 0 && calculatedNumberOfDigits.isEmpty()) {
						// xs:decimal with scale, but without precision in any form -> use a double instead
						return FixedType.DOUBLE;
					}
					int numberOfDigits = calculatedNumberOfDigits.orElse(Integer.MAX_VALUE);
					if (fractionDigits > 0) {
						return DecimalType.withFraction(numberOfDigits, fractionDigits);
					} else {
						// Calculate number of bits; defaulting to Long.SIZE (this coerces unconstrained integers to a Long)
						int numberOfBits = Stream.of(minInclusive, minExclusive, maxInclusive, maxExclusive, maxDigits, minDigits)
								.flatMap(Optional::stream).map(BigDecimal::unscaledValue).map(BigInteger::bitLength)
								.max(Integer::compareTo).map(b -> b + 1) // Plus sign bit
								.orElse(Long.SIZE);
						if (numberOfBits <= Integer.SIZE) {
							return DecimalType.INTEGER_TYPE;
						} else if (numberOfBits <= Long.SIZE) {
							return DecimalType.LONG_TYPE;
						} else {
							return DecimalType.integer(numberOfBits, numberOfDigits);
						}
					}
				} else if (XSD_STRING.equals(recognizedType)) {
					List<String> symbols = restriction(typeInfo, ENUMERATION).toList();
					if (symbols.isEmpty()) {
						return FixedType.STRING;
					} else {
						// typeCollection may be null, as this class prevents duplicate names itself.
						return new EnumType(typeData.name(), typeData.doc(), symbols, null);
					}
				} else if (XSD_ANYURI.equals(recognizedType)) {
					return FixedType.STRING;
				} else if (XSD_HEXBIN.equals(recognizedType)) {
					return FixedType.BINARY_HEX;
				} else if (XSD_BASE64.equals(recognizedType)) {
					return FixedType.BINARY_BASE64;
				} else {
					throw new IllegalArgumentException("Unsupported simple type: " + typeInfo);
				}
			}
			default -> {
				String typeInfoTypeName = typeInfo.getType().name().toLowerCase(Locale.ROOT);
				throw new IllegalArgumentException(format("xs:simpleType with xs:%s is not supported", typeInfoTypeName));
			}
		}
	}

	private static <T> Optional<T> restriction(XmlSchemaTypeInfo typeInfo, XmlSchemaRestriction.Type restrictionType, Function<String, T> converter) {
		return restriction(typeInfo, restrictionType).map(converter).findFirst();
	}

	private static Stream<String> restriction(XmlSchemaTypeInfo typeInfo, XmlSchemaRestriction.Type restrictionType) {
		return Stream.ofNullable(typeInfo.getFacets()).map(m -> m.getOrDefault(restrictionType, emptyList())).flatMap(List::stream).map(
				XmlSchemaRestriction::getValue).map(Object::toString);
	}

	@Override
	public void onExitElement(XmlSchemaElement element, XmlSchemaTypeInfo typeInfo, boolean previouslyVisited) {
		cardinalityStack.pop();
		VisitorContext context = contextStack.pop();

		Type result;
		if (previouslyVisited) {
			result = structureBuilder.repeatedElement(context.typeData);
		} else {
			result = structureBuilder.endElement(context.typeFields);
		}

		VisitorContext parentContext = contextStack.peek();
		if (parentContext == null) {
			this.result = result;
		} else {
			TypeFields parentElementState = parentContext.typeFields;
			structureBuilder.element(parentElementState, context.fieldData, result);
		}
	}

	@Override
	public void onVisitAttribute(XmlSchemaElement element, XmlSchemaAttrInfo attrInfo) {
		VisitorContext context = contextStack.element();

		XmlSchemaAttribute attribute = attrInfo.getAttribute();

		TypeData typeData = typeData(attribute, attribute.getSchemaType());

		Cardinality cardinality = cardinalityStack.element().adjustFor(Cardinality.of(attribute));
		String documentation = extractDocumentation(attribute);
		ScalarType scalarType = mapScalarType(typeData, attrInfo.getType());
		Object defaultValue = requireNonNull(scalarType, "XSD error: attributes always have a scalar type").parse(attribute.getDefaultValue());

		FieldData fieldData = new FieldData(attribute.getName(), documentation, cardinality, scalarType, defaultValue);
		context.addAttribute(fieldData);
	}

	@Override
	public void onEndAttributes(XmlSchemaElement element, XmlSchemaTypeInfo typeInfo) {
		VisitorContext context = contextStack.element();
		TypeFields state = structureBuilder.startElement(context.fieldData, context.typeData, context.typeAttributes);
		context.setTypeFields(state);
	}

	@Override
	public void onEnterSubstitutionGroup(XmlSchemaElement base) {
		throw new IllegalArgumentException("Substitution groups are not supported");
	}

	@Override
	public void onExitSubstitutionGroup(XmlSchemaElement base) {
		// Nothing to do
	}

	@Override
	public void onEnterAllGroup(XmlSchemaAll all) {
		cardinalityStack.push(repetition(Cardinality.of(all)));
	}

	private Cardinality repetition(Cardinality... cardinalities) {
		Cardinality result = cardinalityStack.element();
		for (Cardinality cardinality : cardinalities) {
			result = result.adjustFor(cardinality);
		}
		return result;
	}

	@Override
	public void onExitAllGroup(XmlSchemaAll all) {
		cardinalityStack.pop();
	}

	@Override
	public void onEnterChoiceGroup(XmlSchemaChoice choice) {
		cardinalityStack.push(repetition(Cardinality.OPTIONAL, Cardinality.of(choice)));
	}

	@Override
	public void onExitChoiceGroup(XmlSchemaChoice choice) {
		cardinalityStack.pop();
	}

	@Override
	public void onEnterSequenceGroup(XmlSchemaSequence seq) {
		cardinalityStack.push(repetition(Cardinality.of(seq)));
	}

	@Override
	public void onExitSequenceGroup(XmlSchemaSequence seq) {
		cardinalityStack.pop();
	}

	@Override
	public void onVisitAny(XmlSchemaAny any) {
		TypeFields parentElementState = requireNonNull(contextStack.element(), "'any' element is not supported at toplevel").typeFields;
		structureBuilder.elementContainsAny(parentElementState);
	}

	@Override
	public void onVisitAnyAttribute(XmlSchemaElement element, XmlSchemaAnyAttribute anyAttr) {
		throw new IllegalArgumentException("'any' attributes are not supported");
	}

	private static final class VisitorContext {
		private final FieldData fieldData;
		private final TypeData typeData;
		private final List<FieldData> typeAttributes;
		private TypeFields typeFields;

		private VisitorContext(FieldData fieldData, TypeData typeData) {
			this.fieldData = fieldData;
			this.typeData = typeData;
			typeAttributes = new ArrayList<>();
			this.typeFields = null;
		}

		private void addAttribute(FieldData attributeField) {
			typeAttributes.add(attributeField);
		}

		private void setTypeFields(TypeFields typeFields) {
			this.typeFields = typeFields;
		}
	}
}
