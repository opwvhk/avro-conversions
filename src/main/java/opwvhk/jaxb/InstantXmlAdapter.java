package opwvhk.jaxb;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import static java.time.ZoneOffset.UTC;

public class InstantXmlAdapter
		extends JavaTimeXmlAdapter<Instant> {
	@SuppressWarnings("SpellCheckingInspection")
	private static final DateTimeFormatter ISO_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

	public InstantXmlAdapter() {
		super(Instant::from, ISO_DATE_TIME_FORMATTER);
	}

	@Override
	protected TemporalAccessor temporalAccessor(Instant value) {
		return ZonedDateTime.ofInstant(value, UTC);
	}
}
