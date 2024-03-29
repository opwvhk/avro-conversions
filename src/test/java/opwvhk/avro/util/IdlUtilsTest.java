package opwvhk.avro.util;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdlUtilsTest {
    @Test
    void idlUtilsUtilitiesThrowRuntimeExceptions() {
        assertThatThrownBy(() -> IdlUtils.getField(Object.class, "noSuchField"))
                .isInstanceOf(IllegalStateException.class).hasMessage("Programmer error");
        assertThatThrownBy(() -> IdlUtils.getFieldValue(String.class.getDeclaredField("value"), "anything"))
                .isInstanceOf(IllegalStateException.class).hasMessage("Programmer error");

        assertThatThrownBy(() -> IdlUtils.getMethod(Object.class, "noSuchMethod"))
                .isInstanceOf(IllegalStateException.class).hasMessage("Programmer error");
        assertThatThrownBy(() -> {
            Object foo = IdlUtils.invokeMethod(Object.class.getDeclaredMethod("clone"), new Object());
            System.err.println(foo);
        }).isInstanceOf(IllegalStateException.class).hasMessage("Programmer error");
    }

    @Test
    void validateHappyFlow() throws ParseException, IOException {
        StringWriter schemaBuffer = new StringWriter();
        try (InputStreamReader reader = new InputStreamReader(Objects.requireNonNull(getClass().getResourceAsStream("schema.avdl")))) {
            char[] buf = new char[1024];
            int charsRead;
            while((charsRead = reader.read(buf)) > -1) {
                schemaBuffer.write(buf, 0, charsRead);
            }
        }
        Protocol protocol = new Idl(new StringReader(schemaBuffer.toString())).CompilationUnit();
        // Write as JSON and parse again to handle logical types correctly.
        protocol = Protocol.parse(protocol.toString());
        Schema newMessageSchema = protocol.getType("naming.NewMessage");
		assertThat(protocol.getTypes().stream().map(Schema::getFullName)).contains("naming.NewMessage");

        StringWriter buffer = new StringWriter();
        IdlUtils.writeIdlProtocol("naming", "HappyFlow", buffer, newMessageSchema);

        assertThat(buffer.toString()).isEqualToIgnoringWhitespace(schemaBuffer.toString());

    }

    @Test
    void cannotWriteUnnamedTypes() {
        assertThatThrownBy(() -> IdlUtils.writeIdlProtocol("naming", "Error", new StringWriter(),
                Schema.create(Schema.Type.STRING))).isInstanceOf(AvroRuntimeException.class);
    }

    @Test
    void cannotWriteEmptyEnums() {
        assertThatThrownBy(() -> IdlUtils.writeIdlProtocol("naming", "Error", new StringWriter(),
                Schema.createEnum("Single", null, "naming", emptyList()))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void cannotWriteEmptyUnionTypes() {
        assertThatThrownBy(() -> IdlUtils.writeIdlProtocol("naming", "Error", new StringWriter(),
                Schema.createRecord("Single", null, "naming", false, singletonList(
                        new Schema.Field("field", Schema.createUnion())
                )))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void validateNullToJson() throws IOException {
        assertThat(callToJson(JsonProperties.NULL_VALUE)).isEqualTo("null");
    }

    @Test
    void validateMapToJson() throws IOException {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("key", "name");
        data.put("value", 81763);
        assertThat(callToJson(data)).isEqualTo("{\"key\":\"name\",\"value\":81763}");
    }

    @Test
    void validateCollectionToJson() throws IOException {
        assertThat(callToJson(Arrays.asList(123, "abc"))).isEqualTo("[123,\"abc\"]");
    }

    @Test
    void validateBytesToJson() throws IOException {
        assertThat(callToJson("getalletjes".getBytes(StandardCharsets.US_ASCII))).isEqualTo("\"getalletjes\"");
    }

    @Test
    void validateStringToJson() throws IOException {
        assertThat(callToJson("foo")).isEqualTo("\"foo\"");
    }

    @Test
    void validateEnumToJson() throws IOException {
        assertThat(callToJson(SingleValue.FILE_NOT_FOUND)).isEqualTo("\"FILE_NOT_FOUND\"");
    }

    @Test
    void validateDoubleToJson() throws IOException {
        assertThat(callToJson(25_000.025)).isEqualTo("25000.025");
    }

    @Test
    void validateFloatToJson() throws IOException {
        assertThat(callToJson(15_000.002f)).isEqualTo("15000.002");
    }

    @Test
    void validateLongToJson() throws IOException {
        assertThat(callToJson(7254378234L)).isEqualTo("7254378234");
    }

    @Test
    void validateIntegerToJson() throws IOException {
        assertThat(callToJson(123)).isEqualTo("123");
    }

    @Test
    void validateBooleanToJson() throws IOException {
        assertThat(callToJson(true)).isEqualTo("true");
    }

    @Test
    void validateUnknownCannotBeWrittenAsJson() {
        assertThatThrownBy(() -> callToJson(new Object())).isInstanceOf(AvroRuntimeException.class);
    }

    private String callToJson(Object datum) throws IOException {
        StringWriter buffer = new StringWriter();
        try (JsonGenerator generator = IdlUtils.SCHEMA_FACTORY.createGenerator(buffer)) {
            IdlUtils.toJson(datum, generator);
        }
        return buffer.toString();
    }

    private enum SingleValue {
        FILE_NOT_FOUND
    }
}
