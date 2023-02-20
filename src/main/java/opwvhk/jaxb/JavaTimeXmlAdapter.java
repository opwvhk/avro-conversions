package opwvhk.jaxb;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;

import static java.util.Objects.requireNonNull;

/**
 * <p>{@code XmlAdapter} that maps strings to/from any JSR-310 {@code TemporalAccessor} using a provided
 * {@code DateTimeFormatter}. To use, create a subclass for your specific type.</p>
 */
public abstract class JavaTimeXmlAdapter<T extends TemporalAccessor> extends XmlAdapter<String, T> {
    private final TemporalQuery<? extends T> temporalQuery;
    private final DateTimeFormatter formatter;

    /**
     * @param temporalQuery the temporal query used by the formatter to create an instance of the type to parse
     * @param formatter     the formatter for formatting and parsing the type
     */
    public JavaTimeXmlAdapter(TemporalQuery<? extends T> temporalQuery, DateTimeFormatter formatter) {
        this.temporalQuery = requireNonNull(temporalQuery, "temporalQuery must not be null");
        this.formatter = requireNonNull(formatter, "formatter must not be null");
    }

	/**
	 * Return the {@code TemporalAccessor} to be used for marshalling.
	 *
	 * <p>The default implementation returns the value as-is, but some values may not support all fields the {@code DateTimeFormatter} expects. In that case,
	 * you should override this method.</p>
	 *
	 * @param value the value to marshal
	 * @return a {@code TemporalAccessor} that supports all fields required by the {@code DateTimeFormatter} passed to the constructor
	 */
	protected TemporalAccessor temporalAccessor(T value) {
		return value;
	}

    @Override
    public String marshal(T value) {
        return value != null ? formatter.format(temporalAccessor(value)) : null;
    }

    @Override
    public T unmarshal(String value) {
        return value != null ? formatter.parse(value, temporalQuery) : null;
    }
}
