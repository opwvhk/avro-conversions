package opwvhk.avro.xml.datamodel;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static opwvhk.avro.xml.datamodel.TestStructures.optional;
import static opwvhk.avro.xml.datamodel.TestStructures.required;
import static opwvhk.avro.xml.datamodel.TestStructures.struct;
import static opwvhk.avro.xml.datamodel.TestStructures.unparsed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeStructureTest {
	@Test
	void testAvroDefaultValues() {
		StructType struct = struct("defaults", "Testing default values").withFields(
				required("nullDefaults", struct("nulls").withFields(
						optional("optional1", null, FixedType.STRING, StructType.Field.NULL_VALUE),
						optional("optional2", null, FixedType.STRING, JsonProperties.NULL_VALUE),
						optional("optional3", null, FixedType.STRING, Schema.Field.NULL_DEFAULT_VALUE)
				)),
				optional("optional4", "Only non-null default", FixedType.STRING, "text"));
		assertThat(struct.debugString("")).isEqualTo("""
				StructType(defaults) {
				  Doc: Testing default values
				  nullDefaults
				    StructType(nulls) {
				      optional1?
				        Default: @NULL1@
				        string
				      optional2?
				        Default: @NULL2@
				        string
				      optional3?
				        Default: @NULL3@
				        string
				    }
				  optional4?
				    Doc: Only non-null default
				    Default: text
				    string
				}"""
				.replace("@NULL1@", StructType.Field.NULL_VALUE.toString())
				.replace("@NULL2@", JsonProperties.NULL_VALUE.toString())
				.replace("@NULL3@", Schema.Field.NULL_DEFAULT_VALUE.toString())
		);

		Schema schema = new Schema.Parser().parse("""
				{"type": "record", "name": "defaults", "fields": [
					{"name": "nullDefaults", "type": {"type": "record", "name": "nulls", "fields": [
						{"name": "optional1", "type": ["null", "string"], "default": null},
						{"name": "optional2", "type": ["null", "string"], "default": null},
						{"name": "optional3", "type": ["null", "string"], "default": null}
					]}},
					{"name": "optional4", "type": ["string", "null"], "default": "text"}
				]}""");
		assertThat(struct.toSchema()).isEqualTo(schema);
	}

	@Test
	void testSimpleScalarParsing() {
		// Nulls always succeed
		assertThat(FixedType.BOOLEAN.parse(null)).isNull();
		assertThat(FixedType.FLOAT.parse(null)).isNull();
		assertThat(FixedType.DOUBLE.parse(null)).isNull();
		assertThat(FixedType.DATE.parse(null)).isNull();
		assertThat(FixedType.DATETIME.parse(null)).isNull();
		assertThat(FixedType.TIME.parse(null)).isNull();
		assertThat(FixedType.STRING.parse(null)).isNull();
		assertThat(FixedType.BINARY_HEX.parse(null)).isNull();
		assertThat(FixedType.BINARY_BASE64.parse(null)).isNull();

		// 'simple' types can be parsed
		assertThat(FixedType.BOOLEAN.parse("true")).isEqualTo(true);
		assertThat(FixedType.FLOAT.parse("12.34")).isInstanceOf(Float.class).isEqualTo(12.34f);
		assertThat(FixedType.DOUBLE.parse("12.34")).isInstanceOf(Double.class).isEqualTo(12.34);
		assertThat(FixedType.STRING.parse("some text")).isEqualTo("some text");
		assertThat(FixedType.BINARY_HEX.parse("DEAD")).isEqualTo(ByteBuffer.wrap(bytes(0, 222, 173)));
		assertThat(FixedType.BINARY_BASE64.parse("U2ltcGxlIHRleHQ=")).isEqualTo(ByteBuffer.wrap("Simple text".getBytes(UTF_8)));

		// Date and time values cannot be parsed here (as default values)
		assertThatThrownBy(() -> FixedType.DATE.parse("2023-03-16")).isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> FixedType.DATETIME.parse("2023-03-16T04:49:00.781z")).isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> FixedType.TIME.parse("12:43:56.078")).isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	void testDecimals() {
		DecimalType smallInteger = DecimalType.integer(20, 7);
		assertThat(smallInteger.debugString("")).isEqualTo("decimal(7; 20 bits)");
		DecimalType largeInteger = DecimalType.integer(40, 13);
		assertThat(largeInteger.debugString("")).isEqualTo("decimal(13; 40 bits)");
		DecimalType hugeInteger = DecimalType.integer(70, 22);
		assertThat(hugeInteger.debugString(">>")).isEqualTo(">>decimal(22; 70 bits)");
		DecimalType fractional = DecimalType.withFraction(6, 2);
		assertThat(fractional.debugString("#")).isEqualTo("#decimal(6,2)");
		assertThatThrownBy(() -> new DecimalType(1, 2, 3)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> DecimalType.integer(-12, 12)).isInstanceOf(IllegalArgumentException.class);

		assertThat(smallInteger.parse(null)).isNull();
		assertThat(largeInteger.parse(null)).isNull();
		assertThat(hugeInteger.parse(null)).isNull();
		assertThat(fractional.parse(null)).isNull();

		assertThat(smallInteger.parse("12")).isEqualTo(12);
		assertThat(largeInteger.parse("12")).isEqualTo(12L);
		assertThat(hugeInteger.parse("12")).isEqualTo(BigDecimal.valueOf(12));
		assertThat(fractional.parse("12")).isEqualTo(BigDecimal.valueOf(1200, 2));

		assertThatThrownBy(() -> smallInteger.parse("12.34")).isInstanceOf(NumberFormatException.class);
		assertThatThrownBy(() -> largeInteger.parse("12.34")).isInstanceOf(NumberFormatException.class);
		assertThatThrownBy(() -> hugeInteger.parse("12.34")).isInstanceOf(ArithmeticException.class);
		assertThat(fractional.parse("12.34")).isEqualTo(BigDecimal.valueOf(1234, 2));

		assertThatThrownBy(() -> fractional.parse("12.345")).isInstanceOf(ArithmeticException.class);
		assertThatThrownBy(() -> fractional.parse("123456.78")).isInstanceOf(ArithmeticException.class);
	}

	@Test
	void testEnums() {
		assertThatThrownBy(() -> new EnumType("name", null, List.of("the", "default", "symbol", "must"), "exist")).isInstanceOf(
				IllegalArgumentException.class);

		EnumType withoutDefault = new EnumType("name", "Description", List.of("one", "two"), null);
		assertThat(withoutDefault.defaultSymbol()).isNull();
		assertThat(withoutDefault.documentation()).isEqualTo("Description");
		EnumType withDefault = new EnumType("name", "Description", List.of("one", "two"), "one");
		assertThat(withDefault.defaultSymbol()).isEqualTo("one");
		assertThat(withDefault.documentation()).isEqualTo("Description");

		assertThat(withoutDefault.debugString("")).isEqualTo("enum(name: one, two)");
		assertThat(withDefault.debugString("")).isEqualTo("enum(name: one, two; one)");

		assertThat(withoutDefault.parse(null)).isNull();
		assertThat(withoutDefault.parse("one")).isEqualTo("one");
		assertThat(withoutDefault.parse("two")).isEqualTo("two");
		assertThatThrownBy(() -> withoutDefault.parse("three")).isInstanceOf(IllegalArgumentException.class);

		assertThat(withDefault.parse(null)).isNull();
		assertThat(withDefault.parse("one")).isEqualTo("one");
		assertThat(withDefault.parse("two")).isEqualTo("two");
		assertThat(withDefault.parse("three")).isEqualTo("one");
	}

	@Test
	void testUnparsedContent() {
		Type type = unparsed(FixedType.STRING);
		assertThat(type.toString()).isEqualTo("(unparsed) string");
		assertThat(type.debugString("> ")).isEqualTo("> (unparsed) string");
	}

	@Test
	void testStructuralEdgeCases() {
		StructType type = struct("name", "Testing");
		assertThat(type.toString()).isEqualTo("""
				StructType(name) {
				  Doc: Testing
				  (no fields yet)
				}""");

		StructType.Field field1 = required("field1", FixedType.STRING);
		assertThatThrownBy(() -> type.setFields(List.of(field1, optional("field1", FixedType.FLOAT)))).isInstanceOf(IllegalArgumentException.class);
		type.setFields(List.of(field1, required("field2", FixedType.STRING)));
		assertThat(type.toString()).isEqualTo("""
				StructType(name) {
				  Doc: Testing
				  field1
				    string
				  field2
				    string
				}""");
		assertThat(type.fields()).hasSize(2).contains(field1);

		assertThatThrownBy(() -> type.setFields(type.fields().subList(0, 1))).isInstanceOf(IllegalStateException.class);

		StructType otherType = new StructType("otherType", null);
		assertThatThrownBy(() -> otherType.setFields(type.fields())).isInstanceOf(IllegalStateException.class);

		assertThatThrownBy(() -> new StructType.Field("typeIsRequired", null, Cardinality.REQUIRED, null, null)).isInstanceOf(NullPointerException.class);

		StructType type1 = struct("name1");
		type1.setFields(List.of());
		StructType type2 = struct("name2");
		type2.setFields(List.of());
		StructType type3 = struct("name1", "doc");
		type3.setFields(List.of());
		StructType type4 = struct("name1").withFields(required("field", null, FixedType.STRING, null));
		StructType type5 = struct("name1").withFields(required("field", null, FixedType.STRING, null));

		// noinspection EqualsWithItself
		assertThat(type1).isEqualTo(type1);
		assertThat(type4).isEqualTo(type5);
		assertThat(type1).isNotEqualTo(null);
		// noinspection AssertBetweenInconvertibleTypes
		assertThat(type1).isNotEqualTo("mismatch");
		assertThat(type1).isNotEqualTo(type2);
		assertThat(type1).isNotEqualTo(type3);
		assertThat(type1).isNotEqualTo(type4);
		assertThat(type4).isNotEqualTo(type1);

		assertThat(type1.hashCode()).isEqualTo(type1.hashCode());
		assertThat(type4.hashCode()).isEqualTo(type5.hashCode());

		assertThat(type.fields().get(0).hashCode()).isNotEqualTo(type.fields().get(1).hashCode());
	}

	private static byte[] bytes(int... bytes) {
		byte[] result = new byte[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			result[i] = (byte) bytes[i];
		}
		return result;
	}
}
