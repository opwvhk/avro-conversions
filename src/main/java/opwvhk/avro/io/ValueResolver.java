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
