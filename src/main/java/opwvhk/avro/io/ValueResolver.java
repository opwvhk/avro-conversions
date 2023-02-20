package opwvhk.avro.io;

import java.math.BigInteger;
import java.util.Base64;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/**
 * Class to resolve records with. Assumes records are and/or consist of properties and content.
 *
 * <p>Resolver implementations should be created with some callback mechanism to their caller. After parsing of a record, the method {@link #complete(Object)}
 * is called, allowing results to be communicated back to the caller.</p>
 */
public abstract class ValueResolver {
	public static ValueResolver forSchema(GenericData model, Schema schema) throws ResolvingFailure {
		return switch (schema.getType()) {
			case BOOLEAN -> new ScalarValueResolver(Boolean::parseBoolean);
			case INT -> new ScalarValueResolver(Integer::decode);
			case LONG -> new ScalarValueResolver(Long::decode);
			case FLOAT -> new ScalarValueResolver(Float::parseFloat);
			case DOUBLE -> new ScalarValueResolver(Double::parseDouble);
			case STRING -> new ScalarValueResolver(s -> s);
			case ENUM -> new ScalarValueResolver(symbol -> {
				String enumSymbol = schema.getEnumSymbols().contains(symbol) ? symbol : schema.getEnumDefault();
				if (enumSymbol == null) {
					throw new ParseFailure("Unknown enum symbol for %s: %s".formatted(schema.getFullName(), symbol));
				}
				return model.createEnum(enumSymbol, schema);
			});
			case BYTES -> {
				String binaryType = schema.getProp("xmlBinary");
				if ("hex".equals(binaryType)) {
					yield new ScalarValueResolver(s -> new BigInteger(s, 16).toByteArray());
				} else if ("base64".equals(binaryType)) {
					Base64.Decoder base64Decoder = Base64.getDecoder();
					yield new ScalarValueResolver(base64Decoder::decode);
				} else {
					throw new ResolvingFailure("Binary type '%s' is not supported. Please use 'hex' or 'base64'.".formatted(binaryType));
				}
			}
			case ARRAY -> new ListResolver(ValueResolver.forSchema(model, schema.getElementType()));
			case UNION -> {
				List<Schema> unionSchemas = schema.getTypes();
				if (unionSchemas.size() != 2 || !schema.isNullable()) {
					throw new ResolvingFailure("Unions are only supported to make a single schema nullable.");
				}
				Schema firstSchema = unionSchemas.get(0);
				Schema nonNullSchema = firstSchema.isNullable() ? unionSchemas.get(1) : firstSchema;
				yield ValueResolver.forSchema(model, nonNullSchema);
			}
			case RECORD -> {
				RecordResolver resolver = new RecordResolver(model, schema);
				// TODO: add fields
				yield resolver;
			}
			default -> throw new ResolvingFailure("Schema type '%s' is not supported".formatted(schema.getType()));
		};
	}

	private static final ValueResolver NOOP = new ValueResolver() {
		@Override
		public Object addContent(Object collector, String content) {
			return null;
		}
	};
	private static final ValueResolver NON_PARSING = new ValueResolver() {
		@Override
		public Object addContent(Object collector, String content) {
			return content;
		}

		@Override
		public boolean shouldParseContent() {
			return false;
		}
	};

	/**
	 * Resolve a property of this record and return the resolver to handle it.
	 *
	 * <p>The default implementation returns a resolver that accepts anything, but yields no results. Useful to ignore unknown properties.</p>
	 *
	 * @param name the property name
	 * @return the resolver to handle the property
	 */
	public ValueResolver resolve(String name) {
		return NOOP;
	}

	/**
	 * Create a collector for parsing results.
	 *
	 * <p>Used to pass to {@link #addProperty(Object, String, Object)}, {@link #addContent(Object, String)} and {@link #complete(Object)}.</p>
	 *
	 * <p>The default implementation returns {@literal null}</p>
	 *
	 * @return a new collector instance
	 */
	public Object createCollector() {
		return null;
	}

	/**
	 * Add a property value to the collector.
	 *
	 * <p>The default implementation simply returns the collector.</p>
	 *
	 * @param collector the (current) value collector
	 * @param name      the property name; also used to find the property resolver using {@link #resolve(String)}
	 * @param value     the property value to add, returned by the property resolver
	 * @return the completed collector (possibly a new instance)
	 */
	public Object addProperty(Object collector, String name, Object value) {
		return collector;
	}

	/**
	 * Add the tag content to the collector.
	 *
	 * <p>The default implementation throws an exception.</p>
	 *
	 * @param collector the (current) value collector
	 * @param content   the content of the element
	 * @return the value collector (possibly a new instance) with the new value added
	 */
	public Object addContent(Object collector, String content) {
		throw new IllegalArgumentException("Values are not supported");
	}

	/**
	 * Complete the record, and pass it back to the creator.
	 *
	 * <p>The default implementation simply returns the collector.</p>
	 *
	 * @param collector the (current) value collector
	 * @return the completed record
	 */
	public Object complete(Object collector) {
		return collector;
	}

	public boolean shouldParseContent() {
		return true;
	}
}
