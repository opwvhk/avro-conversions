package opwvhk.avro;

import javax.xml.namespace.QName;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import opwvhk.avro.xsd.Cardinality;
import opwvhk.avro.xsd.FieldData;
import opwvhk.avro.xsd.ScalarType;
import opwvhk.avro.xsd.StructureBuilder;
import opwvhk.avro.xsd.TypeData;
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
import org.apache.ws.commons.schema.walker.XmlSchemaAttrInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaTypeInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaVisitor;
import org.w3c.dom.Node;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StructuralSchemaVisitor<ElementState, Result>
		implements XmlSchemaVisitor {
	private final StructureBuilder<ElementState, Result> structureBuilder;
	private final Function<String, String> xmlToTypeNamespace;
	private final int maxDuplicateClasses;
	private final Deque<Cardinality> cardinalityStack;
	private final Deque<VisitorContext<ElementState>> contextStack;
	private final Set<String> generatedClassNames;
	private Result result;

	public StructuralSchemaVisitor(StructureBuilder<ElementState, Result> structureBuilder, Function<String, String> xmlToTypeNamespace,
	                               int maxDuplicateClasses) {
		this.structureBuilder = structureBuilder;
		this.xmlToTypeNamespace = xmlToTypeNamespace;
		this.maxDuplicateClasses = maxDuplicateClasses;
		cardinalityStack = new ArrayDeque<>();
		cardinalityStack.push(Cardinality.defaultValue());
		contextStack = new ArrayDeque<>();
		generatedClassNames = new HashSet<>();
	}

	public Result result() {
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

		ScalarType scalarType = mapScalarType(typeInfo);
		String defaultValue = scalarType == null ? null : element.getDefaultValue();
		FieldData fieldData = new FieldData(element.getName(), extractDocumentation(element), elementCardinality, scalarType, defaultValue);

		XmlSchemaType schemaType = element.getSchemaType();
		String className = className(element, schemaType);
		TypeData typeData = new TypeData(className, extractDocumentation(schemaType), schemaType.isMixed());

		cardinalityStack.push(Cardinality.defaultValue());
		contextStack.push(new VisitorContext<>(fieldData, typeData, null));
	}

	String className(XmlSchemaElement element, XmlSchemaType schemaType) {
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
		if (XsdAnalyzer.XML_SCHEMA_DEFINITION_NAMESPACES.contains(xmlNamespace)) {
			return null;
		}

		String typeNamespace = xmlToTypeNamespace.apply(xmlNamespace);
		String className;
		if (typeNamespace.isEmpty()) {
			className = name.getLocalPart();
		} else {
			className = typeNamespace + "." + name.getLocalPart();
		}

		if (!mustHaveSuffix && generatedClassNames.add(className)) {
			return className;
		}
		int i=2;
		while (i < maxDuplicateClasses) {
			String extraClassName = className + i++;
			if (generatedClassNames.add(extraClassName)) {
				return extraClassName;
			}
		}
		throw new IllegalStateException("Too many classes named %s; maximum sequence number (%d) reached.".formatted(className, maxDuplicateClasses));
	}

	private ScalarType mapScalarType(XmlSchemaTypeInfo typeInfo) {
		//noinspection EnhancedSwitchMigration -- reason: branch coverage measurement is incomplete
		switch (typeInfo.getType()) {
			case COMPLEX:
				return null;
			case ATOMIC:
				return ScalarType.fromTypeInfo(typeInfo);
			default:
				String typeInfoTypeName = typeInfo.getType().name().toLowerCase(Locale.ROOT);
				throw new IllegalArgumentException(format("xs:simpleType with xs:%s is not supported", typeInfoTypeName));
		}
	}

	@Override
	public void onExitElement(XmlSchemaElement element, XmlSchemaTypeInfo typeInfo, boolean previouslyVisited) {
		cardinalityStack.pop();
		VisitorContext<ElementState> context = contextStack.pop();

		Result result;
		if (previouslyVisited) {
			result = structureBuilder.repeatedElement(context.fieldData, context.typeData);
		} else {
			result = structureBuilder.endElement(context.elementState, context.fieldData, context.typeData);
		}

		VisitorContext<ElementState> parentContext = contextStack.peek();
		if (parentContext == null) {
			this.result = result;
		} else {
			ElementState parentElementState = parentContext.elementState;
			structureBuilder.element(parentElementState, context.fieldData, context.typeData, result);
		}
	}

	@Override
	public void onVisitAttribute(XmlSchemaElement element, XmlSchemaAttrInfo attrInfo) {
		VisitorContext<ElementState> context = contextStack.element();

		XmlSchemaAttribute attribute = attrInfo.getAttribute();
		Cardinality occurrence = cardinalityStack.element().adjustFor(Cardinality.of(attribute));
		XmlSchemaTypeInfo typeInfo = attrInfo.getType();
		ScalarType scalarType = requireNonNull(mapScalarType(typeInfo), "Attributes must have a (simple) type");
		String defaultValue = attribute.getDefaultValue();
		FieldData fieldData = new FieldData(attribute.getName(), extractDocumentation(attribute), occurrence, scalarType, defaultValue);

		context.addAttribute(fieldData);
	}

	@Override
	public void onEndAttributes(XmlSchemaElement element, XmlSchemaTypeInfo typeInfo) {
		VisitorContext<ElementState> context = contextStack.element();
		ElementState state = structureBuilder.startElement(context.fieldData, context.typeData, context.typeAttributes);
		context.setElementState(state);
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
		ElementState parentElementState = requireNonNull(contextStack.element(), "'any' element is not supported at toplevel").elementState;
		structureBuilder.elementContainsAny(parentElementState);
	}

	@Override
	public void onVisitAnyAttribute(XmlSchemaElement element, XmlSchemaAnyAttribute anyAttr) {
		throw new IllegalArgumentException("'any' attributes are not supported");
	}

	static final class VisitorContext<ElementState> {
		private final FieldData fieldData;
		private final TypeData typeData;
		private final List<FieldData> typeAttributes;
		private ElementState elementState;

		public VisitorContext(FieldData fieldData, TypeData typeData, ElementState elementState) {
			this.fieldData = fieldData;
			this.typeData = typeData;
			typeAttributes = new ArrayList<>();
			this.elementState = elementState;
		}

		private void addAttribute(FieldData attributeField) {
			typeAttributes.add(attributeField);
		}

		private void setElementState(ElementState elementState) {
			this.elementState = elementState;
		}

		@Override
		public String toString() {
			return fieldData + " is a " + typeData + " with " + elementState;
		}
	}
}
