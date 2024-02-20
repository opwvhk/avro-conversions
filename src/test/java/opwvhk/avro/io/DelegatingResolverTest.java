package opwvhk.avro.io;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DelegatingResolverTest {

	@Test
	void validateDelegation() {
		DelegatingResolver resolver = new DelegatingResolver();

		MockResolver delegate = new MockResolver();
		resolver.setDelegate(delegate);

		Object collector = new Object();
		Object value = new Object();

		resolver.doNotParseContent();
		assertThat(delegate.calls()).containsExactly("doNotParseContent()");

		resolver.resolve("name");
		assertThat(delegate.calls()).containsExactly("resolve(name)");

		resolver.createCollector();
		assertThat(delegate.calls()).containsExactly("createCollector()");

		resolver.addProperty(collector, "name", value);
		assertThat(delegate.calls()).containsExactly("addProperty(" + collector + ", name, " + value + ")");

		resolver.addContent(collector, "text");
		assertThat(delegate.calls()).containsExactly("addContent(" + collector + ", text)");

		resolver.complete(collector);
		assertThat(delegate.calls()).containsExactly("complete(" + collector + ")");

		resolver.parseContent();
		assertThat(delegate.calls()).containsExactly("parseContent()");
	}

	private static class MockResolver extends ValueResolver {
		private List<String> calls = new ArrayList<>();

		@Override
		public void doNotParseContent() {
			calls.add("doNotParseContent()");
			super.doNotParseContent();
		}

		@Override
		public ValueResolver resolve(String name) {
			calls.add("resolve(%s)".formatted(name));
			return super.resolve(name);
		}

		@Override
		public Object createCollector() {
			calls.add("createCollector()");
			return super.createCollector();
		}

		@Override
		public Object addProperty(Object collector, String name, Object value) {
			calls.add("addProperty(%s, %s, %s)".formatted(collector, name, value));
			return collector;
		}

		@Override
		public Object addContent(Object collector, String content) {
			calls.add("addContent(%s, %s)".formatted(collector, content));
			return collector;
		}

		@Override
		public Object complete(Object collector) {
			calls.add("complete(%s)".formatted(collector));
			return super.complete(collector);
		}

		@Override
		public boolean parseContent() {
			calls.add("parseContent()");
			return super.parseContent();
		}

		private List<String> calls() {
			List<String> result = calls;
			calls = new ArrayList<>();
			return result;
		}
	}
}
