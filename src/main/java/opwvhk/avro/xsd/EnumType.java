package opwvhk.avro.xsd;

import java.util.List;

public record EnumType(List<String> enumSymbols) implements ScalarType {
	@Override
	public String toString() {
		return "enum(%s)".formatted(String.join(", ", enumSymbols));
	}
}
