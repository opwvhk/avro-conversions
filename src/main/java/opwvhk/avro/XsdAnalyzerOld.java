package opwvhk.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.xmlet.xsdparser.core.XsdParser;
import org.xmlet.xsdparser.core.utils.DefaultParserConfig;
import org.xmlet.xsdparser.core.utils.ParserConfig;
import org.xmlet.xsdparser.xsdelements.*;
import org.xmlet.xsdparser.xsdelements.xsdrestrictions.XsdIntegerRestrictions;
import org.xmlet.xsdparser.xsdelements.xsdrestrictions.XsdStringRestrictions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.math.BigDecimal.ONE;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public class XsdAnalyzerOld {
	private static final Pattern UNDERSCORES_PLUS_FOLLOWERS = Pattern.compile("(?U)_([^_])");
	private static final Pattern INITIAL_CAPITALS = Pattern.compile("(?U)^(\\p{Lu}(?!\\p{Lu})|\\p{Lu}+(?=$|\\p{Lu}\\p{Ll}))");
	private static final Pattern INITIAL_LETTER = Pattern.compile("(?U)^(\\p{L})");
	private static final ParserConfig CONFIG = new DefaultParserConfig() {
		@Override
		public Map<String, String> getXsdTypesToJava() {
			Map<String, String> xsdTypesToJava = super.getXsdTypesToJava();
			for (String prefix : asList("xs:", "xsd:")) {
				xsdTypesToJava.put(prefix + "date", LocalDate.class.getName());
				xsdTypesToJava.put(prefix + "dateTime", Instant.class.getName());
				xsdTypesToJava.put(prefix + "time", LocalTime.class.getName());
			}
			return xsdTypesToJava;
		}
	};
	private final List<XsdElement> schemaElements;
	private final Map<String, String> nameOverridesByPaths;
	private final Set<String> classesToKeep;

	/**
	 * Create an XSD analyzer.
	 *
	 * @param schemaFilename the name of the XSD
	 */
	public XsdAnalyzerOld(String schemaFilename) {
		XsdParser xsdParser = new XsdParser(schemaFilename, CONFIG);
		Stream<XsdElement> elementStream = xsdParser.getResultXsdElements();
		this.schemaElements = elementStream.collect(Collectors.toList());
		this.nameOverridesByPaths = new HashMap<>();
		this.classesToKeep = new HashSet<>();
	}

	/**
	 * <p>Lists all names in the XSD, as path from the schema root. Each entry in the result is a concatenation of 1 or more names, prefixed
	 * with a dot (so yes, each entry starts with a dot).</p>
	 *
	 * <p>This method ignores {@link #addNameOverride(String, String) name overrides}, as it is intended to be used to set them.</p>
	 * <p>
	 *
	 * @param rootElement the name of a root element in the XSD
	 * @return all name paths in the XSD
	 */
	public List<String> listNamePaths(String rootElement) {
		LinkedHashSet<String> names = new LinkedHashSet<>();
		readNamePaths(findRootElement(rootElement)).forEach(names::add);
		return new ArrayList<>(names);
	}

	private Stream<String> readNamePaths(XsdElement element) {
		if (element.getXsdComplexType() == null) {
			return Stream.of(namePath(element));
		}

		return processComplexType(element.getXsdComplexType(), this::readNamePaths, type -> {
			// Composite element: read class names from (flattened) child elements
			return concat(Stream.of(namePath(type)), concat(
					complexTypeElements(type).flatMap(this::readNamePaths),
					stream(type.getXsdAttributeGroup(), type.getXsdAttributes()).flatMap(this::readNamePaths))
			);
		});
	}

	private <T> T processComplexType(XsdComplexType type, Function<XsdExtension, T> processExtension,
	                                 Function<XsdComplexType, T> processNestedType) {
		if (type.getSimpleContent() != null) {
			XsdSimpleContent simpleContent = type.getSimpleContent();
			if (simpleContent.getXsdRestriction() != null) {
				throw new IllegalArgumentException("xs:restriction inside xs:simpleContent is not supported");
			}
			return processExtension.apply(simpleContent.getXsdExtension());
		} else if (type.getComplexContent() != null) {
			XsdComplexContent complexContent = type.getComplexContent();
			if (complexContent.getXsdRestriction() != null) {
				throw new IllegalArgumentException("xs:restriction inside xs:complexContent is not supported");
			}
			return processExtension.apply(complexContent.getXsdExtension());
		} else {
			return processNestedType.apply(type);
		}
	}

	private Stream<XsdElement> complexTypeElements(XsdComplexType type) {

		return flatten(type.getXsdChildElement());
	}

	private Stream<XsdElement> flatten(XsdAbstractElement abstractElement) {
		int minOccurs;
		String maxOccurs;
		if (abstractElement instanceof XsdGroup group) {
			minOccurs = group.getMinOccurs();
			maxOccurs = group.getMaxOccurs();
		} else if (abstractElement instanceof XsdAll group) {
			minOccurs = group.getMinOccurs();
			maxOccurs = "" + group.getMaxOccurs();
		} else if (abstractElement instanceof XsdChoice group) {
			minOccurs = group.getMinOccurs();
			maxOccurs = group.getMaxOccurs();
		} else if (abstractElement instanceof XsdSequence group) {
			minOccurs = group.getMinOccurs();
			maxOccurs = group.getMaxOccurs();
		} else if (abstractElement instanceof XsdElement) {
			return Stream.of((XsdElement) abstractElement);
		} else {
			// This only happens if the element is null.
			return Stream.empty();
		}
		if (minOccurs != 1 || !maxOccurs.equals("1")) {
			throw new IllegalArgumentException(
					"Optional or repeating xs:group/xs:all/xs:choice/xs:sequence (minOccurs != 1 || maxOccurs != 1) are not supported");
		}
		// For non-repeating multi-elements
		return abstractElement.getXsdElements().flatMap(this::flatten);
	}

	private Stream<String> readNamePaths(XsdExtension extension) {
		validateExtension(extension);
		String namePath = namePath(extension);
		return concat(Stream.of(namePath, namePath + ".value"),
				stream(extension.getXsdAttributeGroup(), extension.getXsdAttributes()).flatMap(this::readNamePaths));
	}

	private void validateExtension(XsdExtension extension) {
		if (extension.getXsdChildElement() != null) {
			throw new IllegalArgumentException("xs:extention with xs:group, xs:all, xs:choice or xs:sequence is not supported");
		}
		if (extension.getBaseAsComplexType() != null) {
			throw new IllegalArgumentException("xs:extention of xs:complexType (records) is not supported");
		}
	}

	private Stream<String> readNamePaths(XsdAttribute attribute) {
		return Stream.of(namePath(attribute));
	}

	/**
	 * Add a name override for a given name path. Normally, the part after the last dot is used as name. This method allows to override that.
	 *
	 * @param namePath   a name path that is returned from {@link #listNamePaths(String)} (not enforced)
	 * @param forcedName the name to use instead of the part after the last dot
	 */
	public void addNameOverride(String namePath, String forcedName) {
		this.nameOverridesByPaths.put(namePath, forcedName);
	}

	/**
	 * Add the (unqualified) class name to keep when {@link #optimizeSchema(Schema) optimizing} the schema.
	 *
	 * @param classToKeep the name of a class not to optimize away
	 */
	public void addClassToKeep(String classToKeep) {
		classesToKeep.add(classToKeep);
	}

	/**
	 * Create an Avro schema for the given root element.
	 *
	 * @param rootElement the name of a root element in the XSD
	 * @param namespace   the namespace (package) of the Avro schema
	 * @return an Avro schema capable of containing any valid XML of the given root element
	 */
	public Schema createAvroSchema(String rootElement, String namespace) {
		XsdElement element = findRootElement(rootElement);
		Schema schema = createField(namespace, element).schema();
		return optimizeSchema(schema);
	}

	private XsdElement findRootElement(String rootElement) {
		return this.schemaElements.stream()
				.filter(e -> rootElement.equals(e.getName())).findAny()
				.orElseThrow(() -> new IllegalArgumentException("There is no root element " + rootElement + " defined in the XSD"));
	}

	private String doc(XsdAnnotatedElements annotatedElement) {
		String doc = Optional.of(annotatedElement)
				.map(XsdAnnotatedElements::getAnnotation)
				.map(XsdAnnotation::getDocumentations)
				.filter(list -> !list.isEmpty())
				.map(documentations -> {
					StringBuilder buffer = new StringBuilder();
					for (XsdDocumentation documentation : documentations) {
						buffer.append("\n\n").append(documentation.getContent());
					}
					return buffer.substring(2);
				}).orElse(null);
		if (doc == null && !(annotatedElement instanceof XsdElement) && !(annotatedElement instanceof XsdAttribute)) {
			// Only recurse if we have no  documentation yet, but never past elements/attibutes.
			doc = doc((XsdAnnotatedElements) annotatedElement.getParent());
		}
		return doc;
	}

	private String getBaseName(XsdAbstractElement namedElement, boolean tryTypeOverride) {
		String namePath = namePath(requireNonNull(namedElement));
		String forcedTypeName = tryTypeOverride ? nameOverridesByPaths.get(namePath + "@type") : null;
		if (forcedTypeName != null) {
			return forcedTypeName;
		}
		String forcedName = nameOverridesByPaths.get(namePath);
		if (forcedName != null) {
			return forcedName;
		}
		int lastDotPos = namePath.lastIndexOf('.');
		return namePath.substring(lastDotPos + 1);
	}

	private String className(XsdAbstractElement namedElement) {
		return snakeToCamelCase(getBaseName(namedElement, true), true);
	}

	private String fieldName(XsdAbstractElement namedElement) {
		return getBaseName(namedElement, false).toLowerCase(Locale.ROOT);
		//return snakeToCamelCase(getBaseName(namedElement, false), false);
	}

	private String namePath(XsdAbstractElement element) {
		if (element == null) {
			return "";
		}
		String name = null;
		if (element instanceof XsdNamedElements namedElement) {
			name = namedElement.getRawName();
		}
		element.parentAvailable = true; // This makes getParent() work correctly
		if (name == null) {
			return namePath(element.getParent());
		} else {
			return namePath(element.getParent()) + "." + name;
		}
	}

	private Schema.Field createField(String namespace, XsdElement element) {
		Schema schema;
		if (element.getXsdComplexType() != null) {
			schema = processComplexType(element.getXsdComplexType(), ext -> createSchema(namespace, ext),
					type -> createRecord(namespace, type, fields -> {
						stream(type.getXsdAttributeGroup(), type.getXsdAttributes()).forEach(a -> fields.add(createField(a)));
						complexTypeElements(type).forEach(e -> fields.add(createField(namespace, e)));
					}));
		} else if (element.getXsdSimpleType() != null) {
			schema = createSchema(element.getXsdSimpleType());
		} else {
			schema = createSchema(element.getTypeAsBuiltInDataType());
		}
		Cardinality cardinality = Cardinality.of(element);
		return cardinality.createField(fieldName(element), schema, doc(element));
	}

	private Schema.Field createField(XsdAttribute attribute) {
		Schema schema;
		if (attribute.getXsdSimpleType() != null) {
			schema = createSchema(attribute.getXsdSimpleType());
		} else {
			schema = asSchema(CONFIG.getXsdTypesToJava().get(attribute.getType()));
		}
		Cardinality cardinality = Cardinality.of(attribute);
		return cardinality.createField(fieldName(attribute), schema, doc(attribute));
	}

	private Schema asSchema(String javaType) {
		return switch (javaType) {
			case "Boolean" -> Schema.create(Schema.Type.BOOLEAN);
			case "Byte", "Short", "Integer" -> Schema.create(Schema.Type.INT);
			case "Long" -> Schema.create(Schema.Type.LONG);
			case "Float" -> Schema.create(Schema.Type.FLOAT);
			case "Double" -> Schema.create(Schema.Type.DOUBLE);
			case "String" -> Schema.create(Schema.Type.STRING);
			case "java.time.Instant" -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
			case "java.time.LocalDate" -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
			case "java.time.LocalTime" -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
			case "BigDecimal" -> throw new IllegalArgumentException("BigDecimal requires restrictions to derive the scale.");
			case "BigInteger" -> {
				System.out.println("WARNING: BigInteger without restrictions on precision. Using Long instead.");
				yield Schema.create(Schema.Type.LONG);
			}
			default -> throw new IllegalArgumentException("Unsupported Java type: " + javaType);
		};
	}

	private Schema createSchema(XsdBuiltInDataType type) {
		return asSchema(CONFIG.getXsdTypesToJava().get(type.getRawName()));
	}

	private Schema createSchema(XsdSimpleType type) {
		if (type.getList() != null) {
			throw new IllegalArgumentException("xs:list in xs:simpleType is not supported");
		}
		if (type.getUnion() != null) {
			throw new IllegalArgumentException("xs:union in xs:simpleType is not supported");
		}
		return createSchema(type.getRestriction());
	}

	private Schema createSchema(XsdRestriction restriction) {
		if (restriction.getSimpleType() != null) {
			throw new IllegalArgumentException("xs:restriction with a xs:simpleType is not supported");
		}

		String javaType = CONFIG.getXsdTypesToJava().get(restriction.getBase());

		boolean isDecimal = "BigDecimal".equals(javaType);
		if (isDecimal || "BigInteger".equalsIgnoreCase(javaType)) {
			Optional<Integer> fractionDigits = Optional.ofNullable(restriction.getFractionDigits()).map(XsdIntegerRestrictions::getValue);
			if (isDecimal ^ fractionDigits.isPresent()) {
				throw new IllegalArgumentException("xs:fractionDigits is only supported for xs:decimal, and then it's required");
			}
			final int scale = fractionDigits.orElse(0);
			NumberSize numberSize = numericBounds(restriction);

			if (isDecimal) {
				return LogicalTypes.decimal(numberSize.numberOfDigits, scale).addToSchema(Schema.create(Schema.Type.BYTES));
			} else if (numberSize.numberOfBits <= Integer.SIZE) {
				javaType = "Integer";
			} else if (numberSize.numberOfBits <= Long.SIZE) {
				javaType = "Long";
			} else if (numberSize.numberOfBits == Integer.MAX_VALUE) {
				// Special case: coerse an unconstrained xs:integer to Long
				javaType = "Long";
			} else {
				return LogicalTypes.decimal(numberSize.numberOfDigits).addToSchema(Schema.create(Schema.Type.BYTES));
			}
		}

		return asSchema(javaType);
	}

	private static class NumberSize {
		final int numberOfDigits;
		final int numberOfBits;

		public NumberSize(int numberOfDigits, int numberOfBits) {
			this.numberOfDigits = numberOfDigits;
			this.numberOfBits = numberOfBits;
		}
	}

	/**
	 * Determine the number of digits and bits needed to represent any valid number according to the (numeric) restriction.
	 * The number of bits assumes the two's-completent representation of a number with fixed scale.
	 *
	 * @param restriction a restriction on a numeric type
	 * @return the number of bits needed to represent a number of the type
	 */
	private NumberSize numericBounds(XsdRestriction restriction) {
		UnaryOperator<BigDecimal> rescale = Optional.ofNullable(restriction.getFractionDigits())
				.map(XsdIntegerRestrictions::getValue)
				.map(fd -> (UnaryOperator<BigDecimal>) (BigDecimal b) -> b.setScale(fd, RoundingMode.HALF_UP))
				.orElse(UnaryOperator.identity());
		Function<BigDecimal, BigInteger> unscale = rescale.andThen(BigDecimal::unscaledValue);

		// Determine bounds as unscaled numbers.
		Optional<BigInteger> minInclusive = Optional.ofNullable(restriction.getMinInclusive())
				.map(XsdStringRestrictions::getValue).map(BigDecimal::new).map(unscale);
		Optional<BigInteger> minExclusive = Optional.ofNullable(restriction.getMinExclusive())
				.map(XsdStringRestrictions::getValue).map(BigDecimal::new).map(b1 -> b1.add(ONE)).map(unscale);
		Optional<BigInteger> maxInclusive = Optional.ofNullable(restriction.getMaxInclusive())
				.map(XsdStringRestrictions::getValue).map(BigDecimal::new).map(unscale);
		Optional<BigInteger> maxExclusive = Optional.ofNullable(restriction.getMaxExclusive())
				.map(XsdStringRestrictions::getValue).map(BigDecimal::new).map(b -> b.subtract(ONE)).map(unscale);
		Optional<BigInteger> totalDigitsInNines = Optional.ofNullable(restriction.getTotalDigits())
				.map(XsdIntegerRestrictions::getValue).map(digits -> BigInteger.TEN.pow(digits).subtract(BigInteger.ONE));
		List<BigInteger> bounds = Stream.of(minInclusive, minExclusive, maxInclusive, maxExclusive, totalDigitsInNines)
				.filter(Optional::isPresent).map(Optional::get).toList();

		int numberOfDigits = bounds.stream().map(BigDecimal::new).mapToInt(BigDecimal::precision).max().orElse(Integer.MAX_VALUE);
		int numberOfBits = bounds.stream().mapToInt(bi -> bi.bitLength() + 1).max().orElse(Integer.MAX_VALUE);

		return new NumberSize(numberOfDigits, numberOfBits);
	}

	private Schema createSchema(String namespace, XsdExtension extension) {
		validateExtension(extension);

		Schema valueSchema;
		if (extension.getBaseAsSimpleType() != null) {
			valueSchema = createSchema(extension.getBaseAsSimpleType());
		} else {
			valueSchema = createSchema(extension.getBaseAsBuiltInDataType());
		}
		return createRecord(namespace, extension, fields -> {
			fields.add(Cardinality.OPTIONAL.createField("value", valueSchema, null));
			stream(extension.getXsdAttributeGroup(), extension.getXsdAttributes()).forEach(attribute -> fields.add(createField(attribute)));
		});
	}

	private Schema createRecord(String namespace, XsdAnnotatedElements element, Consumer<List<Schema.Field>> fieldsProvider) {
		List<Schema.Field> fields = new ArrayList<>();
		fieldsProvider.accept(fields);
		return Schema.createRecord(className(element), doc(element), namespace, false, fields);
	}

	private Stream<XsdAttribute> stream(Stream<XsdAttributeGroup> groups, Stream<XsdAttribute> attributes) {
		return concat(groups.flatMap(XsdAttributeGroup::getAllXsdAttributeGroups).flatMap(XsdAttributeGroup::getXsdAttributes), attributes);
	}

	/**
	 * <p>Optimize the given schema. This applies the following transformations:</p>
	 *
	 * <ul><li>
	 * Records with only 1 field are replaced by the field value
	 * </li><li>
	 * Nullable arrays are replaced by plain arrays
	 * </li><li>
	 * Arrays of nullable elements are replaced by arrays of non-null elements
	 * </li><li>
	 * For all changed fields, the default values are overridden: arrays default to empty, nullable fields default to null, otherwise the field no longer has a default.
	 * </li></ul>
	 *
	 * @param schema the schema top optimize
	 * @return the optimized schema
	 */
	protected Schema optimizeSchema(Schema schema) {
		Schema.Type type = schema.getType();
		if (type == Schema.Type.RECORD) {
			List<Schema.Field> fields = schema.getFields();
			if (fields.size() == 1 && !classesToKeep.contains(schema.getName())) {
				Schema.Field singleField = fields.get(0);
				return optimizeSchema(singleField.schema());
			}
			for (int i = 0; i < fields.size(); i++) {
				Schema.Field field = fields.get(i);
				Schema newFieldSchema = optimizeSchema(field.schema());
				if (!newFieldSchema.equals(field.schema())) {
					Object newDefault = defaultForArrayOrNullable(newFieldSchema);
					Schema.Field newField = new Schema.Field(field.name(), newFieldSchema, field.doc(), newDefault, field.order());
					fields.set(i, newField);
				}
			}
			return schema;
		} else if (type == Schema.Type.UNION) {// First optimize the unioned types.
			List<Schema> types = schema.getTypes();
			types.replaceAll(s -> nonNullableSchema(optimizeSchema(s)));
			// Then, for unions of null with an array, return the array.
			Schema nonNullableSchema = nonNullableSchema(schema);
			if (nonNullableSchema.getType() == Schema.Type.ARRAY) {
				return optimizeSchema(nonNullableSchema);
			}
			// Otherwise return the optimized union.
			return schema;
		} else if (type == Schema.Type.ARRAY) {// For arrays, return an array of the optimized, non-null type.
			Schema elementType = schema.getElementType();
			Schema optimizedElement = optimizeSchema(elementType);
			Schema nonNullableElement = nonNullableSchema(optimizedElement);
			return Schema.createArray(nonNullableElement);
		} else {
			return schema;
		}
	}

	private Object defaultForArrayOrNullable(Schema newFieldSchema) {
		if (newFieldSchema.getType() == Schema.Type.ARRAY) {
			return Collections.emptyList();
		} else if (newFieldSchema.isNullable()) {
			return Schema.Field.NULL_DEFAULT_VALUE;
		} else {
			return null;
		}
	}

	private Schema nonNullableSchema(Schema schema) {
		if (schema.getType() == Schema.Type.UNION) {
			List<Schema> types = schema.getTypes();
			// We only create unions of null with another type.
			return nonNullableSchema(types.get(1));
		}
		return schema;
	}

	public static Schema sortFields(Schema schema) {
		if (schema.getType() == Schema.Type.RECORD) {
			List<Schema.Field> sortedFields = new ArrayList<>();
			for (Schema.Field field : schema.getFields()) {
				sortedFields.add(new Schema.Field(field, sortFields(field.schema())));
			}
			sortedFields.sort(Comparator.comparing(Schema.Field::name));
			return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), sortedFields);
		}
		return schema;
	}

	static String snakeToCamelCase(String basename, boolean initialCapital)  {
		Function<MatchResult, String> firstGroupToUpperCase = m -> m.group(1).toUpperCase(Locale.ROOT);
		Function<MatchResult, String> firstGroupToLowerCase = m -> m.group(1).toLowerCase(Locale.ROOT);

		String withInitialCapital = INITIAL_LETTER.matcher(basename).replaceAll(firstGroupToUpperCase);
		String capitalizedCamelCase = UNDERSCORES_PLUS_FOLLOWERS.matcher(withInitialCapital).replaceAll(firstGroupToUpperCase);
		return initialCapital ? capitalizedCamelCase : INITIAL_CAPITALS.matcher(capitalizedCamelCase).replaceAll(firstGroupToLowerCase);
	}

	private enum Cardinality {
		REQUIRED {
			@Override
			public Schema.Field createField(String name, Schema schema, String doc) {
				return new Schema.Field(name, schema, doc, null);
			}
		}, OPTIONAL {
			@Override
			public Schema.Field createField(String name, Schema schema, String doc) {
				Schema nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
				return new Schema.Field(name, nullableSchema, doc, Schema.Field.NULL_DEFAULT_VALUE);
			}
		}, REPEATED {
			@Override
			public Schema.Field createField(String name, Schema schema, String doc) {
				Schema arraySchema = Schema.createArray(schema);
				return new Schema.Field(name, arraySchema, doc, Collections.emptyList());
			}
		}, NONE {
			@Override
			public Schema.Field createField(String name, Schema schema, String doc) {
				throw new IllegalArgumentException("Cannot create a field that does not occur");
			}
		};

		public static Cardinality of(XsdElement element) {
			String maxOccurs = Optional.ofNullable(element.getMaxOccurs()).orElse("1");
			Integer minOccurs = Optional.ofNullable(element.getMinOccurs()).orElse(1);
			if (!maxOccurs.equals("1")) {
				return REPEATED;
			} else if (/*maxOccurs.equals("1") && */minOccurs == 0) {
				return OPTIONAL;
			} else /*if (maxOccurs.equals("1") && minOccurs == 1) */ {
				return REQUIRED;
			}
		}

		public static Cardinality of(XsdAttribute attribute) {
			String attributeUse = attribute.getUse();
			if ("prohibited".equals(attributeUse)) {
				return NONE;
			} else if ("required".equals(attributeUse)) {
				return REQUIRED;
			} else {
				return OPTIONAL;
			}
		}

		public abstract Schema.Field createField(String name, Schema schema, String doc);
	}
}
