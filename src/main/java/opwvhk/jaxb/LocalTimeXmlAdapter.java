package opwvhk.jaxb;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * JAXB adapter for LocalTime instances.
 */
public class LocalTimeXmlAdapter extends JavaTimeXmlAdapter<LocalTime> {
	/**
	 * Create an adapter.
	 */
    public LocalTimeXmlAdapter() {
        super(LocalTime::from, DateTimeFormatter.ISO_LOCAL_TIME);
    }
}
