package opwvhk.avro.xml;

import java.util.ArrayDeque;
import java.util.Deque;

import opwvhk.avro.io.ValueResolver;
import org.xml.sax.Attributes;

import static java.util.Objects.requireNonNullElse;

class XmlRecordHandler implements SimpleContentHandler {
	private final ValueResolver rootHandler;
	private final Deque<HandlerContext> contextStack;
	private Object value;

	XmlRecordHandler(ValueResolver rootHandler) {
		this.rootHandler = rootHandler;
		contextStack = new ArrayDeque<>();
		value = null;
	}

	public <T> T getValue() {
		return (T) value;
	}

	@Override
	public void startDocument() {
		contextStack.clear();
		value = null;
	}

	@Override
	public void endDocument() {
		// Nothing to do.
	}

	@Override
	public boolean startElement(String uri, String localName, String qName, Attributes attributes) {
		HandlerContext parentContext = contextStack.peek();
		HandlerContext context;
		if (parentContext == null) {
			context = new HandlerContext(rootHandler);
		} else {
			String element = requireNonNullElse(localName, qName);
			context = parentContext.resolve(element);
		}
		contextStack.push(context);

		for (int i = 0; i < attributes.getLength(); i++) {
			String attribute = requireNonNullElse(attributes.getLocalName(i), attributes.getQName(i));
			Object attrValue = context.resolveValue(attribute, attributes.getValue(i));
			context.addProperty(attribute, attrValue);
		}

		return context.shouldParseContent();
	}

	@Override
	public void endElement(String uri, String localName, String qName) {
		HandlerContext context = contextStack.pop();
		Object value = context.complete();

		HandlerContext parentContext = contextStack.peek();
		if (parentContext != null) {
			String element = requireNonNullElse(localName, qName);
			parentContext.addProperty(element, value);
		} else {
			this.value = value;
		}
	}

	@Override
	public void characters(CharSequence chars) {
		HandlerContext context = contextStack.element();
		context.appendChars(chars);
	}

	private static class HandlerContext {
		private final ValueResolver resolver;
		private final StringBuilder buffer;
		private Object collector;

		private HandlerContext(ValueResolver resolver) {
			this.resolver = resolver;
			buffer = new StringBuilder();
			collector = resolver.createCollector();
		}

		private boolean shouldParseContent() {
			return resolver.parseContent();
		}

		private HandlerContext resolve(String name) {
			return new HandlerContext(resolver.resolve(name));
		}

		private Object resolveValue(String name, String value) {
			ValueResolver childResolver = resolver.resolve(name);
			Object childCollector = childResolver.createCollector();
			childCollector = childResolver.addContent(childCollector, value);
			return childResolver.complete(childCollector);
		}

		private void addProperty(String name, Object value) {
			collector = resolver.addProperty(collector, name, value);
		}

		private Object complete() {
			String bufferContent = buffer.toString();
			buffer.setLength(0);

			String indentedBufferContent = bufferContent.replaceAll("^\\s*?\\R","").stripTrailing();
			String unindentedBufferContent = indentedBufferContent.stripIndent();
			String content = resolver.parseContent() ? unindentedBufferContent.strip() : unindentedBufferContent;
			if (!content.isEmpty()) {
				collector = resolver.addContent(collector, content);
			}

			collector = resolver.complete(collector);
			return collector;
		}

		private void appendChars(CharSequence chars) {
			buffer.append(chars);
		}
	}
}
