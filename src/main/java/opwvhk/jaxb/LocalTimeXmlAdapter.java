package opwvhk.jaxb;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class LocalTimeXmlAdapter extends JavaTimeXmlAdapter<LocalTime> {
    public LocalTimeXmlAdapter() {
        super(LocalTime::from, DateTimeFormatter.ISO_LOCAL_TIME);
    }
}
