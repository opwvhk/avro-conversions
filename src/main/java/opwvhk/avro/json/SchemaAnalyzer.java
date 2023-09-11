package opwvhk.avro.json;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.jimblackler.jsonschemafriend.GenerationException;
import net.jimblackler.jsonschemafriend.SchemaStore;
import net.jimblackler.jsonschemafriend.Validator;
import opwvhk.avro.io.AsAvroParserBase;
import opwvhk.avro.io.ValueResolver;
import opwvhk.avro.util.AvroSchemaUtils;
import opwvhk.avro.util.DecimalRange;
import opwvhk.avro.util.Utils;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static opwvhk.avro.util.Utils.first;
import static opwvhk.avro.util.Utils.require;

/**
 * Class to analyze JSON schemata for parsing JSON into structured records.
 *
 * <p>NOTE: there is some validation in the JSON schemata, but even a valid JSON schema may reject all JSON documents, or be otherwise nonsensical. In such
 * cases, the result of analysing the JSON schema is undefined. Examples of such undefined behaviour include conflicting formats, or values whose set of allowed
 * types is something other than a single type (optionally with null).</p>
 */
public class SchemaAnalyzer {
    /**
     * Logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaAnalyzer.class);

    private static final Pattern MATCH_NAME_IN_REFERENCE = Pattern.compile(
            ".*#/(?:\\$defs|definitions).*?/(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)" +
            "(?:/(?:allOf|anyOf|oneOf)/\\d+|dependentRequired/[^/]+|if|then|else|prefixItems|items|additionalItems|contains)?");
    private static final Pattern MATCH_NAME_FROM_ANY_ID = Pattern.compile(
            ".*/(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)");

    private final Validator validator;
    private final SchemaStore schemaStore;

    /**
     * Create a new JSON schema analyzer.
     */
    public SchemaAnalyzer() {
        schemaStore = new SchemaStore(true);
        validator = new Validator(true);
    }

    /**
     * Parse a JSON schema and return an Avro schema that matches it.
     *
     * @param jsonSchemaLocation the location of a JSON schema
     * @return an equivalent Avro schema
     * @throws GenerationException when the JSON schema cannot be analysed
     */
    public Schema parseJsonSchema(URI jsonSchemaLocation) throws GenerationException {
        SchemaProperties schemaProperties = parseJsonProperties(jsonSchemaLocation);
        IdentityHashMap<SchemaProperties, Schema> seenResults = new IdentityHashMap<>();
        return asAvroSchema(schemaProperties, seenResults);
    }

    private Schema asAvroSchema(SchemaProperties schemaProperties, IdentityHashMap<SchemaProperties, Schema> seenResults) {
        Schema previousResult = seenResults.get(schemaProperties);
        if (previousResult != null) {
            return previousResult;
        }
        schemaProperties.isNullable(); // Ensure the type set is not null.
        EnumSet<SchemaType> types = schemaProperties.types();
        if (schemaProperties.properties().isEmpty()) {
            types.remove(SchemaType.OBJECT);
        }
        if (schemaProperties.itemSchemaProperties() == null) {
            types.remove(SchemaType.ARRAY);
        }
        if (!schemaProperties.isIntegerNumberRange()) {
            types.remove(SchemaType.INTEGER);
        }
        if (schemaProperties.getType() == null) {
            throw new IllegalArgumentException("Invalid type definition: no applicable types for " + schemaProperties);
        }
        SchemaType type = schemaProperties.getType();

        Schema schema = switch (type) {
            case OBJECT -> {
                String name = requireNonNull(schemaProperties.title(), "Object types require a name");
                String doc = schemaProperties.description();
                Set<String> requiredProperties = schemaProperties.requiredProperties();
                Schema recordSchema = Schema.createRecord(name, doc, null, false);
                seenResults.put(schemaProperties, recordSchema);
                List<Schema.Field> fields = new ArrayList<>();
                for (Map.Entry<String, SchemaProperties> entry : Utils.requireNonEmpty(schemaProperties.properties(),
                        "Object types require properties").entrySet()) {
                    String fieldName = entry.getKey();
                    SchemaProperties fieldProperties = entry.getValue();
                    Schema fieldSchema = asAvroSchema(fieldProperties, seenResults);
                    String fieldDoc = fieldProperties.description();
                    String defaultValue = fieldProperties.defaultValue();
                    Object parsedDefault;
                    boolean optionalField = fieldSchema.isNullable() || !requiredProperties.contains(fieldName);
                    fieldSchema = AvroSchemaUtils.nonNullableSchemaOf(fieldSchema);
                    if (defaultValue == null) {
                        parsedDefault = optionalField ? Schema.Field.NULL_DEFAULT_VALUE : null;
                    } else {
                        // Assume a scalar value
                        ValueResolver resolver = new AsAvroParserBase<>(GenericData.get()) {
                            @Override
                            public ValueResolver createResolver(Schema readSchema) {
                                return super.createResolver(readSchema);
                            }
                        }.createResolver(fieldSchema);
                        parsedDefault = resolver.complete(resolver.addContent(resolver.createCollector(), defaultValue));
                    }
                    if (optionalField) {
                        if (defaultValue != null) {
                            fieldSchema = Schema.createUnion(fieldSchema, Schema.create(Schema.Type.NULL));
                        } else {
                            fieldSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
                        }
                    }
                    fields.add(new Schema.Field(fieldName, fieldSchema, fieldDoc, parsedDefault));
                }
                recordSchema.setFields(fields);
                yield recordSchema;
            }
            case ARRAY -> Schema.createArray(
                    asAvroSchema(requireNonNull(schemaProperties.itemSchemaProperties(), "Array types require an item schema"), seenResults));
            case STRING -> {
                Set<String> enumValues = schemaProperties.enumValues();
                String format = schemaProperties.format();
                String contentEncoding = schemaProperties.contentEncoding();
                if (enumValues != null) {
                    String name = requireNonNull(schemaProperties.title(), "Enum types require a name");
                    String doc = schemaProperties.description();
                    yield Schema.createEnum(name, doc, null, new ArrayList<>(enumValues), schemaProperties.defaultValue());
                } else if ("date".equals(format)) {
                    yield LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                } else if ("time".equals(format)) {
                    yield LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
                } else if ("date-time".equals(format)) {
                    yield LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                } else if ("base64".equals(contentEncoding) || "base16".equals(contentEncoding)) {
                    yield Schema.create(Schema.Type.BYTES);
                } else {
                    if (format != null) {
                        LOGGER.warn("Unsupported format ({}); using string instead", format);
                    } else if (contentEncoding != null) {
                        LOGGER.warn("Unsupported content encoding ({}); using string instead", contentEncoding);
                    }
                    yield Schema.create(Schema.Type.STRING);
                }
            }
            case INTEGER -> {
                DecimalRange range = require(schemaProperties, SchemaProperties::isIntegerNumberRange,
                        "Integer numbers require an integer number range").numberRange();
                int bitSize = range.integerBitSize();
                if (bitSize == 0) {
					// Not specified: use LONG instead of throwing an error
                    yield Schema.create(Schema.Type.LONG);
                } else if (bitSize <= 32) {
                    yield Schema.create(Schema.Type.INT);
                } else if (bitSize <= 64) {
                    yield Schema.create(Schema.Type.LONG);
                } else {
                    yield LogicalTypes.decimal(range.requiredPrecision()).addToSchema(Schema.create(Schema.Type.BYTES));
                }
            }
            case NUMBER -> {
                DecimalRange range = schemaProperties.numberRange();
                int precision = range.requiredPrecision();
                if (precision == 0) {
	                // Not specified: use DOUBLE instead of throwing an error
                    yield Schema.create(Schema.Type.DOUBLE);
                } else if (precision < 7) {
                    // Floats have a precision of ~7 digits
                    yield Schema.create(Schema.Type.FLOAT);
                } else if (precision < 16) {
                    // Doubles have a precision of ~16 digits
                    yield Schema.create(Schema.Type.DOUBLE);
                } else {
                    yield LogicalTypes.decimal(precision, range.requiredScale()).addToSchema(Schema.create(Schema.Type.BYTES));
                }
            }
            // Note: the getType() method above never returns null
            default /*BOOLEAN*/ -> Schema.create(Schema.Type.BOOLEAN);
        };
        if (schemaProperties.isNullable()) {
            if (schemaProperties.defaultValue() != null) {
                return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
            } else {
                return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
            }
        } else {
            return schema;
        }
    }

    /**
     * Parse a JSON schema and return the schema properties describing objects that can contain JSON data adhering to this schema.
     *
     * @param jsonSchemaLocation the location of a JSON schema
     * @return an object describing the JSON schema
     * @throws GenerationException when the JSON schema cannot be analysed
     */
    public SchemaProperties parseJsonProperties(URI jsonSchemaLocation) throws GenerationException {
        // Load/parse the schema.
        net.jimblackler.jsonschemafriend.Schema schema = schemaStore.loadSchema(jsonSchemaLocation, validator);

        // Interpret the schema

        URI metaSchema = schema.getMetaSchema();
        SchemaVersion schemaVersion = SchemaVersion.valueOf(metaSchema);

        Map<URI, SchemaProperties> examinedSchemas = new HashMap<>();
        return determineSchemaProperties(schema, schemaVersion, examinedSchemas);
    }

    private SchemaProperties determineSchemaProperties(net.jimblackler.jsonschemafriend.Schema schema, SchemaVersion version,
                                                       Map<URI, SchemaProperties> examinedSchemas) {
        SchemaProperties schemaProperties = new SchemaProperties(version.isAtLeast(SchemaVersion.DRAFT_6));
        SchemaProperties existing = examinedSchemas.putIfAbsent(schema.getUri(), schemaProperties);
        if (existing != null) {
            return existing;
        }

        Collection<String> explicitTypes = schema.getExplicitTypes();
        if (explicitTypes != null) {
            // explicitTypes is either null or contains at least one element
            EnumSet<SchemaType> types = EnumSet.noneOf(SchemaType.class);
            for (String s : explicitTypes) {
                SchemaType schemaType = SchemaType.ofName(s);
                types.add(schemaType);
            }
            schemaProperties.setTypes(types);
        }

        schemaProperties.setInferredTitle(inferTitle(schema));
        schemaProperties.setTitle(schema.getTitle());
        schemaProperties.setDescription(schema.getDescription());

        schemaProperties.setDefaultValue(schema.getDefault());

        schemaProperties.setFormat(schema.getFormat());
        if (version.isAtLeast(SchemaVersion.DRAFT_7)) {
            schemaProperties.setContentEncoding(schema.getContentEncoding());
        }

        determineNumberRange(schemaProperties, schema, version);
        determineEnumValues(schemaProperties, schema, version);
        schemaProperties.setItemSchemaProperties(determineArrayItemSchema(schema, version, examinedSchemas));

        schemaProperties.requiredProperties().addAll(schema.getRequiredProperties());

        for (Map.Entry<String, net.jimblackler.jsonschemafriend.Schema> entry : schema.getProperties().entrySet()) {
            schemaProperties.addProperty(entry.getKey(), determineSchemaProperties(entry.getValue(), version, examinedSchemas));
        }

        List<net.jimblackler.jsonschemafriend.Schema> optionalSchemas = new ArrayList<>();
        add(optionalSchemas, schema.getAnyOf());
        add(optionalSchemas, schema.getOneOf());
        optionalSchemas.addAll(schema.getDependentSchemas().values());
        if (version.isAtLeast(SchemaVersion.DRAFT_7)) {
            add(optionalSchemas, schema.getIf());
            add(optionalSchemas, schema.getThen());
            add(optionalSchemas, schema.getElse());
        }

        Function<net.jimblackler.jsonschemafriend.Schema, SchemaProperties> determineProperties = s -> determineSchemaProperties(s, version, examinedSchemas);
        List<SchemaProperties> requiredSchemas = new ArrayList<>();
        Optional.ofNullable(version.isAtLeast(SchemaVersion.DRAFT_2020_12) ? schema.getRef() : null)
                .map(determineProperties).ifPresent(requiredSchemas::add);
        optionalSchemas.stream()
                .map(determineProperties)
                .reduce((sp1, sp2) -> combineSchemaProperties(CombineType.UNION, sp1, sp2))
                .ifPresent(requiredSchemas::add);
        schema.getAllOf().stream().map(determineProperties).forEach(requiredSchemas::add);
        for (SchemaProperties other : requiredSchemas) {
            combineSchemaProperties(CombineType.INTERSECT, schemaProperties, other);
        }

        // Infer additionally allowed types from properties that are set.

        if (!schemaProperties.properties().isEmpty()) {
            schemaProperties.addType(SchemaType.OBJECT);
        }
        if (schemaProperties.itemSchemaProperties() != null) {
            schemaProperties.addType(SchemaType.ARRAY);
        }
        if (first(schemaProperties.format(), schemaProperties.contentEncoding()) != null || schemaProperties.enumValues() != null) {
            schemaProperties.addType(SchemaType.STRING);
        }
        if (schemaProperties.numberRange().upperBound() != null || schemaProperties.numberRange().lowerBound() != null) {
            schemaProperties.addType(SchemaType.NUMBER);
            if (schemaProperties.isIntegerNumberRange()) {
                schemaProperties.addType(SchemaType.INTEGER);
            }
        }
        // No types set, nor inferred: all types are allowed.
        if (schemaProperties.types() == null) {
            schemaProperties.setTypes(EnumSet.allOf(SchemaType.class));
        }

        return schemaProperties;
    }

    private static String inferTitle(net.jimblackler.jsonschemafriend.Schema schema) {
        return Stream.of(schema, schema.getRef())
                .filter(Objects::nonNull)
                .map(net.jimblackler.jsonschemafriend.Schema::getResourceUri)
                .map(Object::toString)
                .flatMap(location -> Stream.of(MATCH_NAME_IN_REFERENCE, MATCH_NAME_FROM_ANY_ID)
                        .map(p -> p.matcher(location))
                        .filter(Matcher::matches)
                        .map(m -> m.group(1)))
                .findFirst().orElse(null);
    }

    private static void determineNumberRange(SchemaProperties schemaProperties, net.jimblackler.jsonschemafriend.Schema schema, SchemaVersion version) {
        if (version.isAtLeast(SchemaVersion.DRAFT_6)) {
            schemaProperties.setNumberRange(
                    new DecimalRange(bd(schema.getMinimum()), bd(schema.getExclusiveMinimum()), bd(schema.getMaximum()), bd(schema.getExclusiveMaximum())));
        } else {
            BigDecimal minimum = bd(schema.getMinimum());
            boolean inclusiveMinimum = isInclusiveBounds(minimum, schema.isExclusiveMinimumBoolean());
            BigDecimal maximum = bd(schema.getMaximum());
            boolean inclusiveMaximum = isInclusiveBounds(maximum, schema.isExclusiveMaximumBoolean());
            schemaProperties.setNumberRange(new DecimalRange(minimum, inclusiveMinimum, maximum, inclusiveMaximum));
        }
    }

    private static boolean isInclusiveBounds(BigDecimal bounds, boolean forceExclusive) {
        return bounds != null && !forceExclusive;
    }

    private static void determineEnumValues(SchemaProperties schemaProperties, net.jimblackler.jsonschemafriend.Schema schema, SchemaVersion version) {
        Set<String> enumValues = Stream.concat(
                schema.hasConst() && version.isAtLeast(SchemaVersion.DRAFT_6) ? Stream.of(schema.getConst()) : Stream.empty(),
                Stream.ofNullable(schema.getEnums()).flatMap(List::stream)
        ).map(String::valueOf).collect(Collectors.toCollection(LinkedHashSet::new));
        if (!enumValues.isEmpty()) {
            schemaProperties.setEnumValues(enumValues);
        }
    }

    private SchemaProperties determineArrayItemSchema(net.jimblackler.jsonschemafriend.Schema schema, SchemaVersion version,
                                                      Map<URI, SchemaProperties> examinedSchemas) {
        List<net.jimblackler.jsonschemafriend.Schema> itemSchemata = new ArrayList<>();

        net.jimblackler.jsonschemafriend.Schema items = schema.getItems();
        add(itemSchemata, items);
        add(itemSchemata, schema.getItemsTuple());
        if (version.isAtLeast(SchemaVersion.DRAFT_2020_12)) {
            add(itemSchemata, schema.getPrefixItems());
        } else if (items == null) {
            add(itemSchemata, schema.getAdditionalItems());
        }
        if (version.isAtLeast(SchemaVersion.DRAFT_6)) {
            add(itemSchemata, schema.getContains());
        }
        if (version.isAtLeast(SchemaVersion.DRAFT_2019_09)) {
            add(itemSchemata, schema.getUnevaluatedItems());
        }

        return combineUnion(itemSchemata, version, examinedSchemas);
    }

    private SchemaProperties combineUnion(List<net.jimblackler.jsonschemafriend.Schema> schemaList, SchemaVersion version,
                                          Map<URI, SchemaProperties> examinedSchemas) {
        return schemaList.stream()
                .map(s -> determineSchemaProperties(s, version, examinedSchemas))
                .reduce((sp1, sp2) -> combineSchemaProperties(CombineType.UNION, sp1, sp2))
                .orElse(null);
    }

    private SchemaProperties combineSchemaProperties(CombineType combineType, SchemaProperties currentProperties, SchemaProperties extraProperties) {
        // First combine the (possible) types: we'll use the result to merge the other properties
        Set<SchemaType> typesToCopy; // Will be set to all types in currentTypes that have been added from extraProperties.
        if (currentProperties.types() == null) {
            currentProperties.setTypes(extraProperties.types());
            typesToCopy = extraProperties.types();
        } else if (combineType == CombineType.INTERSECT) {
            currentProperties.types().retainAll(extraProperties.types());
            typesToCopy = currentProperties.types();
        } else {
            currentProperties.types().addAll(extraProperties.types());
            typesToCopy = extraProperties.types();
        }

        if (typesToCopy.contains(SchemaType.OBJECT)) {
            if (combineType == CombineType.INTERSECT) {
                currentProperties.requiredProperties().addAll(extraProperties.requiredProperties());
            } else /* combineType == union */ {
                currentProperties.requiredProperties().retainAll(extraProperties.requiredProperties());
            }
            for (Map.Entry<String, SchemaProperties> entry : extraProperties.properties().entrySet()) {
                SchemaProperties currentPropertySchema = currentProperties.properties().get(entry.getKey());
                if (currentPropertySchema == null) {
                    currentProperties.addProperty(entry.getKey(), entry.getValue());
                } else {
                    combineSchemaProperties(combineType, currentPropertySchema, entry.getValue());
                }
            }
        }
        if (typesToCopy.contains(SchemaType.ARRAY)) {
            if (currentProperties.itemSchemaProperties() == null) {
                currentProperties.setItemSchemaProperties(extraProperties.itemSchemaProperties());
            } else {
                combineSchemaProperties(combineType, currentProperties.itemSchemaProperties(), extraProperties.itemSchemaProperties());
            }
        }
        if (typesToCopy.contains(SchemaType.STRING)) {
            currentProperties.setFormat(first(currentProperties.format(), extraProperties.format()));
            currentProperties.setContentEncoding(first(currentProperties.contentEncoding(), extraProperties.contentEncoding()));
            if (combineType == CombineType.INTERSECT) {
                if (currentProperties.enumValues() == null) {
                    currentProperties.setEnumValues(extraProperties.enumValues());
                } else if (extraProperties.enumValues() != null) {
                    currentProperties.enumValues().retainAll(extraProperties.enumValues());
                }
            } else /* combineType == union */ {
                if (extraProperties.enumValues() == null) {
                    currentProperties.setEnumValues(null);
                } else if (currentProperties.enumValues() != null) {
                    currentProperties.enumValues().addAll(extraProperties.enumValues());
                }
            }
        }
        if (typesToCopy.contains(SchemaType.INTEGER) || typesToCopy.contains(SchemaType.NUMBER)) {
            if (combineType == CombineType.INTERSECT) {
                currentProperties.setNumberRange(currentProperties.numberRange().restrictTo(extraProperties.numberRange()));
            } else {
                currentProperties.setNumberRange(currentProperties.numberRange().extendWith(extraProperties.numberRange()));
            }
        }

        currentProperties.setTitle(first(currentProperties.title(), extraProperties.title()));
        currentProperties.setDescription(first(currentProperties.description(), extraProperties.description()));
        currentProperties.setDefaultValue(first(currentProperties.defaultValue(), extraProperties.defaultValue()));
        return currentProperties;
    }

    private static BigDecimal bd(Number number) {
        return number == null ? null : new BigDecimal(number.toString());
    }

    private static <T> void add(List<T> list, Collection<T> newItems) {
        Stream.ofNullable(newItems).flatMap(Collection::stream).forEach(list::add);
    }

    private static <T> void add(List<T> list, T newItem) {
        Stream.ofNullable(newItem).forEach(list::add);
    }

    private enum SchemaVersion {
        DRAFT_3("http://json-schema.org/draft-03/schema#"),
        DRAFT_4("http://json-schema.org/draft-04/schema#"),
        DRAFT_6("http://json-schema.org/draft-06/schema#"),
        DRAFT_7("http://json-schema.org/draft-07/schema#"),
        DRAFT_2019_09("https://json-schema.org/draft/2019-09/schema"),
        DRAFT_2020_12("https://json-schema.org/draft/2020-12/schema");

        private static SchemaVersion valueOf(URI identifier) {
            for (SchemaVersion schemaVersion : values()) {
                if (schemaVersion.identifier.equals(identifier)) {
                    return schemaVersion;
                }
            }
            // Unknown schema: use a default
            return DRAFT_7;
        }

        private final URI identifier;

        SchemaVersion(String identifier) {
            this.identifier = URI.create(identifier);
        }

        private boolean isAtLeast(SchemaVersion schemaVersion) {
            return compareTo(schemaVersion) >= 0;
        }
    }

    private enum CombineType {
        INTERSECT, UNION
    }
}
