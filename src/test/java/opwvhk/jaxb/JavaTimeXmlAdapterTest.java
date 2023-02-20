package opwvhk.jaxb;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import org.junit.Test;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class JavaTimeXmlAdapterTest {
	@Test
	public void testInstantXmlAdapter()
			throws Exception {
		ZonedDateTime dateTime = ZonedDateTime.of(2023, 3, 16, 4, 49, 1, 0, UTC);
		verifyAdapter(new InstantXmlAdapter(), dateTime.toInstant(), "2023-03-16T04:49:01Z");
	}

	@Test
	public void testLocalDateXmlAdapter()
			throws Exception {
		verifyAdapter(new LocalDateXmlAdapter(), LocalDate.of(2023, 3, 16), "2023-03-16");
	}

	@Test
	public void testLocalTimeXmlAdapter()
			throws Exception {
		verifyAdapter(new LocalTimeXmlAdapter(), LocalTime.of(4, 49, 1), "04:49:01");
	}

	private <T> void verifyAdapter(XmlAdapter<String, T> adapter, T value, String formatted)
			throws Exception {
		assertThat(adapter.marshal(null)).isNull();
		assertThat(adapter.unmarshal(null)).isNull();

		assertThat(adapter.marshal(value)).isEqualTo(formatted);
		assertThat(adapter.unmarshal(formatted)).isEqualTo(value);
	}
}
