package opwvhk.jaxb;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * JAXB adapter for LocalTime instances.
 */
public class LocalDateXmlAdapter extends JavaTimeXmlAdapter<LocalDate> {
	/**
	 * Create an adapter.
	 */
    public LocalDateXmlAdapter() {
        super(LocalDate::from, DateTimeFormatter.ISO_LOCAL_DATE);
    }
}
