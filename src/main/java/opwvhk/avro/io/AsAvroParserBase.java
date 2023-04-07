package opwvhk.avro.io;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

import opwvhk.avro.util.AvroSchemaUtils;
import opwvhk.avro.xml.AvroConversions;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * <p>Base class to create parsers for data formats into Avro.</p>
 *
 * <p>Subclasses should implement a </p>
 */
public class AsAvroParserBase {
    /**
     * Date format as specified by ISO8601.
     */
    private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral("-")
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter(Locale.ROOT);
    private static final DateTimeFormatter ZONE_LESS_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(":")
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(":")
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .toFormatter(Locale.ROOT);
    private static final DateTimeFormatter ZONE_LESS_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DATE_FORMAT)
            .optionalStart()
            .appendLiteral("T")
            .optionalEnd()
            .optionalStart()
            .appendLiteral(" ")
            .optionalEnd()
            .append(ZONE_LESS_TIME_FORMATTER)
            .toFormatter(Locale.ROOT);
    /**
     * Resolver for boolean values.
     */
    protected static final ScalarValueResolver BOOLEAN_RESOLVER = new ScalarValueResolver(Boolean::valueOf);
    /**
     * Resolver for single precision floating point values.
     */
    protected static final ScalarValueResolver FLOAT_RESOLVER = new ScalarValueResolver(Float::valueOf);
    /**
     * Resolver for doubl;e precision floating point values.
     */
    protected static final ScalarValueResolver DOUBLE_RESOLVER = new ScalarValueResolver(Double::valueOf);
    /**
     * Resolver for 32-bit signed integer values.
     */
    protected static final ScalarValueResolver INTEGER_RESOLVER = new ScalarValueResolver(Integer::decode);
    /**
     * Resolver for 64-bit signed integer (long) values.
     */
    protected static final ScalarValueResolver LONG_RESOLVER = new ScalarValueResolver(Long::decode);
    /**
     * Resolver for string values.
     */
    protected static final ScalarValueResolver STRING_RESOLVER = new ScalarValueResolver(s -> s);
    /**
     * Resolver for ISO8601 (local) dates (using {@link DateTimeFormatter#ISO_DATE}).
     */
    protected static final ScalarValueResolver LOCAL_DATE_RESOLVER = new ScalarValueResolver(text -> LocalDate.parse(text, DATE_FORMAT));
    /**
     * Resolver for local date-times (using {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME}).
     */
    protected static final ScalarValueResolver LOCAL_DATE_TIME_RESOLVER = new ScalarValueResolver(LocalDateTime::parse);
    private final GenericData model;
    /**
     * Resolver for ISO8601 times (format {@code HH:mm:ss[,SSS][V]}). When a timezone is not specified, the default timezone will be used.
     * Also, times are parsed up to nanosecond level, even though many parsed formats allow any precision.
     */
    protected final ScalarValueResolver offsetTimeResolver;
    /**
     * Resolver for ISO8601 timestamps (format {@code yyyy-MM-dd'T'HH:mm:ss[,SSS][V]}). When a timezone is not specified, the default timezone will be used.
     * Also, times are parsed up to nanosecond level, even though many parsed formats allow any precision.
     */
    protected final ScalarValueResolver instantResolver;

    /**
     * Create an {@code AsAvroParserBase}, using the specified model and the {@code UTC} time zone.
     *
     * @param model the model to create records and enum symbols with
     * @see #AsAvroParserBase(GenericData, ZoneId)
     */
    protected AsAvroParserBase(GenericData model) {
        this(model, UTC);
    }

    /**
     * Create an {@code AsAvroParserBase}, using the specified model and default time zone. The time zone will be used when parsing times and timestamps, if the
     * parsed string does not contain a timestamp.
     *
     * @param model           the model to create records and enum symbols with
     * @param defaultTimezone the default time zone to use when parsing times and timestamps
     */
    protected AsAvroParserBase(GenericData model, ZoneId defaultTimezone) {
        this.model = model;
        DateTimeFormatter timeFormat = ZONE_LESS_TIME_FORMATTER.withZone(requireNonNull(defaultTimezone));
        offsetTimeResolver = new ScalarValueResolver(text -> OffsetTime.parse(text, timeFormat));
        DateTimeFormatter dateTimeFormat = ZONE_LESS_DATE_TIME_FORMATTER.withZone(defaultTimezone);
        instantResolver = new ScalarValueResolver(text -> ZonedDateTime.parse(text, dateTimeFormat).toInstant());

        ensureConversionFor(LogicalTypes.decimal(1, 1), BigDecimal.class, Conversions.DecimalConversion::new);
        ensureConversionFor(LogicalTypes.date(), LocalDate.class, TimeConversions.DateConversion::new);
        ensureConversionFor(LogicalTypes.timestampMillis(), Instant.class, TimeConversions.TimestampMillisConversion::new);
        ensureConversionFor(LogicalTypes.timestampMicros(), Instant.class, TimeConversions.TimestampMicrosConversion::new);
        ensureConversionFor(LogicalTypes.localTimestampMillis(), LocalDateTime.class, TimeConversions.LocalTimestampMillisConversion::new);
        ensureConversionFor(LogicalTypes.localTimestampMicros(), LocalDateTime.class, TimeConversions.LocalTimestampMicrosConversion::new);
        // Note: for each of the logical types, the last conversion becomes the default for the logical type (if queried without class).
        ensureConversionFor(LogicalTypes.timeMillis(), LocalTime.class, TimeConversions.TimeMillisConversion::new);
        ensureConversionFor(LogicalTypes.timeMillis(), OffsetTime.class, AvroConversions.OffsetTimeMillisConversion::new);
        ensureConversionFor(LogicalTypes.timeMicros(), LocalTime.class, TimeConversions.TimeMicrosConversion::new);
        ensureConversionFor(LogicalTypes.timeMicros(), OffsetTime.class, AvroConversions.OffsetTimeMicrosConversion::new);
    }

    private void ensureConversionFor(LogicalType logicalType, Class<?> valueClass, Supplier<Conversion<?>> conversionSupplier) {
        if (model.getConversionByClass(valueClass, logicalType) == null) {
            model.addLogicalTypeConversion(conversionSupplier.get());
        }
    }

    /**
     * <p>Create a resolver for the read schema, assuming the data that will be parsed is compatible.</p>
     *
     * <p>Using this method is an easy way to create a resolver, but has several downsides:</p>
     *
     * <ul>
     *
     * <li>There is no early detection of incompatible types; exceptions due to incompatibilities will happen (at best) while parsing, but can occur later.</li>
     *
     * <li>
     *     Specifically, it is impossible to determine if required fields may be missing. This will not cause problems when parsing, but will cause problems
     *     (exceptions) when using the parse result at a later point in time.
     * </li>
     *
     * <li>The resolver cannot handle binary data</li>
     *
     * </ul>
     *
     * @param readSchema the read schema (schema of the resulting records)
     * @return a resolver that can parse compatible data
     */
    protected ValueResolver createResolver(Schema readSchema) {
        Schema nonNullSchema = AvroSchemaUtils.nonNullableSchemaOf(readSchema);

        LogicalType logicalType = nonNullSchema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
            return createDecimalResolver(readSchema);
        } else if (logicalType instanceof LogicalTypes.Date) {
            return LOCAL_DATE_RESOLVER;
        } else if (logicalType instanceof LogicalTypes.TimeMillis || logicalType instanceof LogicalTypes.TimeMicros) {
            return offsetTimeResolver;
        } else if (logicalType instanceof LogicalTypes.TimestampMillis || logicalType instanceof LogicalTypes.TimestampMicros) {
            return instantResolver;
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
            return LOCAL_DATE_TIME_RESOLVER;
        }

        // No (supported) logical type: parse raw value

        return switch (nonNullSchema.getType()) {
            case BOOLEAN -> BOOLEAN_RESOLVER;
            case FLOAT -> FLOAT_RESOLVER;
            case DOUBLE -> DOUBLE_RESOLVER;
            case INT -> INTEGER_RESOLVER;
            case LONG -> LONG_RESOLVER;
            case STRING -> STRING_RESOLVER;
            case RECORD -> createRecordResolver(readSchema);
            case ENUM -> createEnumResolver(nonNullSchema);
            case ARRAY -> new ListResolver(createResolver(nonNullSchema.getElementType()));
            default -> throw new IllegalArgumentException("Unsupported Avro type: " + nonNullSchema.getType());
        };
    }

    /**
     * Create a resolver for enumerated values.
     *
     * @param enumSchema an Avro schema with type {@link org.apache.avro.Schema.Type#ENUM}.
     * @return a resolver for enumerated values
     */
    protected ScalarValueResolver createEnumResolver(Schema enumSchema) {
        return new ScalarValueResolver(symbol -> enumSymbol(symbol, enumSchema));
    }

    /**
     * Create a resolver for decimal values.
     *
     * @param readSchema an Avro schema with logical type "decimal"
     * @return a resolver for decimal values
     */
    protected ScalarValueResolver createDecimalResolver(Schema readSchema) {
        return new ScalarValueResolver(decimalParser(readSchema));
    }

    private ValueResolver createRecordResolver(Schema readSchema) {
        RecordResolver resolver = new RecordResolver(model, readSchema);
        for (Schema.Field readField : readSchema.getFields()) {
            ValueResolver fieldResolver = createResolver(readField.schema());
            resolver.addResolver(readField.name(), readField.pos(), fieldResolver);
            readField.aliases().forEach(name -> resolver.addResolver(name, readField.pos(), fieldResolver));
        }
        return resolver;
    }

    private Function<String, Object> decimalParser(Schema readSchema) {
        LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) readSchema.getLogicalType();
        int scale = logicalType.getScale();
        // Note: as the XML was validated before parsing, we're certain the precision is not too large.
        return text -> new BigDecimal(text).setScale(scale, RoundingMode.UNNECESSARY);
    }

    private Object enumSymbol(String input, Schema enumSchema) {
        String symbol;
        if (enumSchema.getEnumSymbols().contains(input)) {
            symbol = input;
        } else {
            symbol = requireNonNull(enumSchema.getEnumDefault(), "Invalid symbol for enum without default: " + input);
        }
        return model.createEnum(symbol, enumSchema);
    }
}
