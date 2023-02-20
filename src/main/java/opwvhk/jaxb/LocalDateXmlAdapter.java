package opwvhk.jaxb;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LocalDateXmlAdapter extends JavaTimeXmlAdapter<LocalDate> {
    public LocalDateXmlAdapter() {
        super(LocalDate::from, DateTimeFormatter.ISO_LOCAL_DATE);
    }
}
