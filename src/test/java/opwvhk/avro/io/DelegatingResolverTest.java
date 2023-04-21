package opwvhk.avro.io;

import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DelegatingResolverTest {

    @Test
    public void validateDelegation() {
        ValueResolver delegate = mock(ValueResolver.class);
        DelegatingResolver resolver = new DelegatingResolver();

        Object collector = new Object();
        Object value = new Object();

        assertThat(resolver.hasDelegate()).isFalse();

        resolver.setDelegate(delegate);
        assertThat(resolver.hasDelegate()).isTrue();

        resolver.doNotParseContent();
        verify(delegate).doNotParseContent();
        verifyNoMoreInteractions(delegate);

        resolver.resolve("name");
        verify(delegate).resolve("name");
        verifyNoMoreInteractions(delegate);

        resolver.createCollector();
        verify(delegate).createCollector();
        verifyNoMoreInteractions(delegate);

        resolver.addProperty(collector, "name", value);
        verify(delegate).addProperty(collector, "name", value);
        verifyNoMoreInteractions(delegate);

        resolver.addContent(collector, "text");
        verify(delegate).addContent(collector, "text");
        verifyNoMoreInteractions(delegate);

        resolver.complete(collector);
        verify(delegate).complete(collector);
        verifyNoMoreInteractions(delegate);

        resolver.parseContent();
        verify(delegate).parseContent();
        verifyNoMoreInteractions(delegate);
    }
}
