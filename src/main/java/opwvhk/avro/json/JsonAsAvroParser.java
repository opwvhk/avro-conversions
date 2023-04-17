package opwvhk.avro.json;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Deque;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import opwvhk.avro.io.AsAvroParserBase;
import opwvhk.avro.io.ListResolver;
import opwvhk.avro.io.RecordResolver;
import opwvhk.avro.io.ValueResolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import static java.util.Objects.requireNonNull;

/**
 * <p>JSON parser to read Avro records.</p>
 *
 * <p>This parser requires at least an Avro schema, and yields a parser that can parse any JSON that has a compatible structure.</p>
 *
 * <p>When reading JSON, any conversion that "fits" is supported. Apart from exact (scalar) type matches and matching record fields by name/alias, this
 * includes widening conversions and enum values as string. Additionally, any number can be read as float/double (losing some precision).</p>
 *
 * <p>Note that without a JSON schema, all downsides listed in {@link AsAvroParserBase#createResolver(Schema)} apply.</p>
 */
public class JsonAsAvroParser extends AsAvroParserBase {

    private final ValueResolver resolver;

    /**
     * Create a JSON parser using only the specified Avro schema. The parse result will match the schema, but might be invalid: no check is done that all
     * required fields have a value.
     *
     * @param readSchema the read schema (schema of the resulting records)
     * @param model      the Avro model used to create records
     */
    public JsonAsAvroParser(Schema readSchema, GenericData model) {
        super(model);
        this.resolver = createResolver(readSchema);
    }

    /**
     * Parse the given source into records.
     *
     * @param <T>    the record type
     * @param source JSON data that was read already
     * @return the parsed record
     * @throws IOException when the JSON cannot be read
     */
    public <T> T parse(String source) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (JsonParser parser = mapper.createParser(source)) {
            return parse(parser, resolver);
        }
    }

    /**
     * Parse the given source into records.
     *
     * @param url a location to read JSON data from
     * @param <T> the record type
     * @return the parsed record
     * @throws IOException when the JSON cannot be read
     */
    public <T> T parse(URL url) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (JsonParser parser = mapper.createParser(url)) {
            return parse(parser, resolver);
        }
    }

    private <T> T parse(JsonParser parser, ValueResolver rootResolver) throws IOException {
        CollectingResolver noopResolver = new CollectingResolver(ValueResolver.NOOP);
        Deque<CollectingResolver> parseStack = new ArrayDeque<>();
        parseStack.push(new CollectingResolver(rootResolver));
        T result = null;

        JsonToken jsonToken;
        while ((jsonToken = parser.nextToken()) != null) {
            // We'll never get these values:
            // 'NOT_AVAILABLE' -> only returned by non-blocking parsers, which we don't use
            // 'VALUE_EMBEDDED_OBJECT' -> no known parser ever returns this
            CollectingResolver collectingResolver = parseStack.element();
            switch (jsonToken) {
                case START_OBJECT:
                    if (collectingResolver.isCollectingRecord()) {
                        parseStack.push(noopResolver);
                    } else {
                        JsonLocation location = parser.getTokenLocation();
                        throw new IllegalStateException("Did not expect an object at %d:%d".formatted(location.getLineNr(), location.getColumnNr()));
                    }
                    break;
                case START_ARRAY:
                    if (collectingResolver.isCollectingArray()) {
                        parseStack.push(collectingResolver.resolve("value")); // Any value will do
                    } else {
                        JsonLocation location = parser.getTokenLocation();
                        throw new IllegalStateException("Did not expect an array at %d:%d".formatted(location.getLineNr(), location.getColumnNr()));
                    }
                    break;
                case FIELD_NAME:
                    parseStack.pop();
                    // collectingResolver is the one we just popped; we need its parent
                    parseStack.push(parseStack.element().resolve(parser.currentName()));
                    break;
                default:
                    collectingResolver.addContent(parser.getValueAsString());
                    parseStack.push(noopResolver);
                    // Fall through
                case END_OBJECT:
                case END_ARRAY:
                    parseStack.pop();
                    CollectingResolver valueResolver = parseStack.pop();
                    // Here, we want both the current top collector (the parent), and collector we just popped (that collected our value).
                    // Note we've first popped a dummy/item/field resolver that is used for child elements, if present, but we no longer need that.
                    Object value = valueResolver.complete();
                    CollectingResolver parentResolver = parseStack.peek();
                    if (parentResolver == null) {
                        result = (T) value;
                    } else if (parentResolver.addProperty(parser.currentName(), value)) {
                        parseStack.push(parentResolver.resolve("value")); // Any value will do
                    } else {
                        parseStack.push(noopResolver);
                    }
                    break;
            }
        }
        return result;
    }

    private static class CollectingResolver {
        private final ValueResolver resolver;
        private Object collector;

        private CollectingResolver(ValueResolver resolver) {
            this.resolver = requireNonNull(resolver);
            this.collector = resolver.createCollector();
        }

        private CollectingResolver resolve(String fieldName) {
            return new CollectingResolver(resolver.resolve(fieldName));
        }

        private void addContent(String value) {
            collector = resolver.addContent(collector, value);
        }

        private Object complete() {
            return resolver.complete(collector);
        }

        private boolean addProperty(String fieldName, Object value) {
            resolver.addProperty(collector, fieldName, value);
            return isCollectingArray();
        }

        private boolean isCollectingArray() {
            return resolver instanceof ListResolver;
        }

        private boolean isCollectingRecord() {
            return resolver instanceof RecordResolver;
        }
    }
}
