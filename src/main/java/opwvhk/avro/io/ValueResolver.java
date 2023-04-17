package opwvhk.avro.io;

/**
 * Class to resolve records with. Assumes records are and/or consist of properties and content.
 *
 * <p>Resolver instances are intended to be stateless/immutable, and are constructed before parsing any data. This includes reusing instances in recursive
 * (and thus theoretically infinite) definitions, so the resolvers cannot cache records while resolving them. Instead, they create "collectors" to collect the
 * data in while resolving records, and complete the records into a final result to be communicated back to the caller.</p>
 */
public abstract class ValueResolver {
    /**
     * Dummy resolver that accepts anything but yields no results.
     */
    public static final ValueResolver NOOP = new ValueResolver() {
        @Override
        public Object addContent(Object collector, String content) {
            return null;
        }
    };

    private boolean parseContent = true;

    /**
     * Mark that this resolver is for source data that should not be parsed, even though it can be. This is a feature that is useful for data that contains
     * other data of an unknown structure, like a message envelope.
     */
    public void doNotParseContent() {
        parseContent = false;
    }

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
        throw new IllegalStateException("This resolver should not be called here: the type resolution has a bug");
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

    /**
     * Whether the content for this resolver should be parsed.
     *
     * @return whether this resolver expects structured content ({@code true}), or not ({@code false})
     */
    public boolean parseContent() {
        return parseContent;
    }

}
