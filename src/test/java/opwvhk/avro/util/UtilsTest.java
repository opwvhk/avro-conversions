package opwvhk.avro.util;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UtilsTest {

	@Test
	public void digest() {
		assertThat(Base64.getEncoder().encodeToString(Utils.digest("md5").digest(new byte[0]))).isEqualTo("1B2M2Y8AsgTpgAmY7PhCfg==");
		assertThatThrownBy(() -> Utils.digest("foobar")).isInstanceOf(IllegalArgumentException.class).hasMessage("Unknown algorithm");
	}

	@Test
	public void findFirstNonNullParameter() {
		assertThat(Utils.first(1, null, null)).isEqualTo(1);
		assertThat(Utils.first(null, 2, null)).isEqualTo(2);
		assertThat(Utils.first(null, null, 3)).isEqualTo(3);

		assertThat(Utils.<String>first(null, null, null)).isNull();
		assertThat(Utils.<String>first()).isNull();
	}

	@Test
	public void truncatingText() {
		assertThat(Utils.truncate(10, null)).isNull();

		assertThat(Utils.truncate(30, "This is a simple sentence.")).isEqualTo("This is a simple sentence.");
		assertThat(Utils.truncate(20, "This is a simple sentence.")).isEqualTo("This is a simple se…");
		assertThat(Utils.truncate(10, "This is a simple sentence.")).isEqualTo("This is a…");
	}

	@Test
	public void testRecursiveEquality() {
		Dummy dummy1a = new Dummy("name", null);
		Dummy dummy1b = new Dummy("name", null);
		Dummy dummy2 = new Dummy("other", null);

		assertThat(dummy1a).isNotEqualTo(null);
		// noinspection AssertBetweenInconvertibleTypes
		assertThat(dummy1a).isNotEqualTo("mismatch");
		assertThat(dummy1a).isEqualTo(dummy1b);
		assertThat(dummy1a).isNotEqualTo(dummy2);

		assertThat(dummy1a.hashCode()).isEqualTo(dummy1b.hashCode());
		assertThat(dummy1a.hashCode()).isNotEqualTo(dummy2.hashCode());

		Dummy bert = new Dummy("Bert", null);
		Dummy ernie = new Dummy("Ernie", bert);
		bert.setManager(ernie);

		Dummy bert2 = new Dummy("Bert", null);
		Dummy ernie2 = new Dummy("Ernie", bert2);
		bert2.setManager(ernie2);

		assertThat(bert).isNotEqualTo(ernie);
		assertThat(bert).isEqualTo(bert2);
		// Most important here is that these don't run out of stack
		assertThat(bert.hashCode()).isEqualTo(bert2.hashCode());

		// noinspection AssertBetweenInconvertibleTypes
		assertThat(new Utils.Seen(12, null)).isNotEqualTo("mismatch");
		// noinspection EqualsWithItself
		assertThat(new Utils.Seen(12, 12)).isEqualTo(new Utils.Seen(12, 12));
		assertThat(new Utils.Seen(12, 12)).isNotEqualTo(new Utils.Seen(12, 21));
		assertThat(new Utils.Seen(12, 12)).isNotEqualTo(new Utils.Seen(21, 12));
		assertThat(new Utils.Seen(12, 12)).isNotEqualTo(new Utils.Seen(21, 21));
	}

	@Test
	public void testRecursionFailure() {
		assertThatThrownBy(() -> Utils.nonRecursive("test", this, null, () -> {
			throw new RuntimeException("Oops...");
		})).isInstanceOf(RuntimeException.class).hasMessage("Oops...");
	}

	@Test
	public void testRequirementsCheckers() {
		Utils.require(true, Objects::nonNull, "Good");
		assertThatThrownBy(() -> Utils.require(null, Objects::nonNull, "foo")).isInstanceOf(IllegalArgumentException.class);

		Utils.requireNonEmpty(Map.of("key", "value"), "Good");
		assertThatThrownBy(() -> Utils.requireNonEmpty(null, "foo")).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> Utils.requireNonEmpty(Collections.emptyMap(), "foo")).isInstanceOf(IllegalArgumentException.class);
	}

	@SuppressWarnings({"FieldCanBeLocal", "unused"})
	private static class Dummy {
		private final String name;
		private Dummy manager;

		private Dummy(String name, Dummy manager) {
			this.name = name;
			this.manager = manager;
		}

		private String name() {
			return name;
		}

		private Dummy manager() {
			return manager;
		}

		private void setManager(Dummy manager) {
			this.manager = manager;
		}

		@Override
		public int hashCode() {
			// noinspection NonFinalFieldReferencedInHashCode
			return Utils.recursionSafeHashCode(this, name, manager);
		}

		@Override
		@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
		public boolean equals(Object obj) {
			return Utils.recursionSafeEquals(this, obj, Dummy::name, Dummy::manager);
		}
	}
}
