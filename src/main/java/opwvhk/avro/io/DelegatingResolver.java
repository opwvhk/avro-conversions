package opwvhk.avro.io;

/**
 * Empty {@code ValueResolver} that delegates all method calls. Useful to provide a resolver while preventing infinite recursion.
 */
public class DelegatingResolver extends ValueResolver {
    private ValueResolver delegate;

    /**
     * Set the delegate for this resolver.
     *
     * @param delegate the delegate to use
     */
    public void setDelegate(ValueResolver delegate) {
        this.delegate = delegate;
    }

    @Override
    public void doNotParseContent() {
        delegate.doNotParseContent();
    }

    @Override
    public ValueResolver resolve(String name) {
        return delegate.resolve(name);
    }

    @Override
    public Object createCollector() {
        return delegate.createCollector();
    }

    @Override
    public Object addProperty(Object collector, String name, Object value) {
        return delegate.addProperty(collector, name, value);
    }

    @Override
    public Object addContent(Object collector, String content) {
        return delegate.addContent(collector, content);
    }

    @Override
    public Object complete(Object collector) {
        return delegate.complete(collector);
    }

    @Override
    public boolean parseContent() {
        return delegate.parseContent();
    }
}
