package opwvhk.avro.util;

import java.io.StringReader;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

public class AvroSchemaUtilsTest {
    @Test
    public void testMarkdownTable()
            throws ParseException {
        //language=Avro IDL
        String idlSchema = """
                protocol dummy {
                	/**
                	 * A documented record
                	 */
                	record MainRecord {
                		/** An optional field */
                		Nested? optional;
                		/**
                		 * A financial journal (list of transactions).
                		 */
                		array<Transaction> journal;
                		map<int> labelledNumbers;
                		/** This is an unlikely field, containing either text or a number. */
                		union { string, int } textOrNumber;
                	}
                	/**
                	 * A nested record
                	 */
                	record Nested {
                		/**
                		 * The title is required.
                		 */
                		string title;
                		/**
                		 * A description is not required.
                		 */
                		string? description;
                	}
                	/**
                	 * A financial transaction.
                	 */
                	record Transaction {
                		/** When the payment was made */
                		timestamp_ms when;
                		/** The payment amount (in EUR) */
                		decimal(9,2) howMuch;
                		/** Who paid the money */
                		string fromWhom;
                		/** Who received the money */
                		string toWhom;
                		/** Why the payment was made */
                		string? why;
                	}
                }
                """;
        //language=Markdown
        @SuppressWarnings("MarkdownIncorrectTableFormatting") String expectedResult = """
                | Field(path) | Type | Documentation |
                |-------------|------|---------------|
                |  | record | Type: A documented record |
                | optional? | record | An optional field<br/>Type: A nested record |
                | optional?.title | string | The title is required. |
                | optional?.description? | string | A description is not required. |
                | journal[] | record | A financial journal (list of transactions).<br/>Type: A financial transaction. |
                | journal[].when | timestamp-millis | When the payment was made |
                | journal[].howMuch | decimal(9,2) | The payment amount (in EUR) |
                | journal[].fromWhom | string | Who paid the money |
                | journal[].toWhom | string | Who received the money |
                | journal[].why? | string | Why the payment was made |
                | labelledNumbers() | int |  |
                | textOrNumber | string | This is an unlikely field, containing either text or a number. |
                | textOrNumber | int | This is an unlikely field, containing either text or a number. |
                """;

        Schema schema = new Idl(new StringReader(idlSchema)).CompilationUnit().getType("MainRecord");
        String result = AvroSchemaUtils.documentAsMarkdown(schema);

        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void checkDocumentationNewlinesAreHtml() {
        String textWithNewlines = "Line 1\nLine 2\nLine 3";
        AvroSchemaUtils.Entry entry = new AvroSchemaUtils.Entry("path.to.entry", "test", textWithNewlines);
        assertThat(entry.documentation()).isEqualTo(textWithNewlines);
        assertThat(entry.toString()).isEqualTo("| path.to.entry | test | Line 1<br/>Line 2<br/>Line 3 |\n");
    }

    @Test
    public void testDuplicateNameDetection() throws ParseException {
        String idlSchema = """
                @namespace("ns")
                protocol dummy {
                    @aliases(["one","two"])
                    record WithDuplicates {
                        one? @aliases(["extra"]) first;
                        array<two> @aliases(["extra"]) second;
                        Good @aliases(["good_field"]) third;
                        string fourth;
                    }
                
                    enum one {on, off}
                
                    fixed two(16);
                
                    record Good {
                        string description;
                        Good rabbitHole;
                    }
                }
                """;
        Protocol protocol = new Idl(new StringReader(idlSchema)).CompilationUnit();

        Schema withDuplicateNames = protocol.getType(protocol.getNamespace() + ".WithDuplicates");
        List<String> namesInProtocol = protocol.getTypes().stream().map(Schema::getFullName).toList();
        assertThat(namesInProtocol)
                .containsExactlyInAnyOrder("ns.WithDuplicates", "ns.one", "ns.two", "ns.Good");

        assertThat(withDuplicateNames).isNotNull();
        assertThatThrownBy(() -> AvroSchemaUtils.requireUniqueNames(withDuplicateNames))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("ns.one", "ns.two", "extra")
                .hasMessageNotContainingAny("ns.Good", "first", "second", "third", "fourth", "description", "rabbitHole");

        Schema good = protocol.getType("ns.Good");
        assertThat(catchThrowable(() -> AvroSchemaUtils.requireUniqueNames(good))).isNull();
    }

    @Test
    public void testUnwrappingNullableUnion() {
        Schema nonUnionSchema = Schema.create(Schema.Type.STRING);
        assertThat(AvroSchemaUtils.nonNullableSchemaOf(nonUnionSchema)).isSameAs(nonUnionSchema);

        Schema union1 = Schema.createUnion(nonUnionSchema, Schema.create(Schema.Type.NULL));
        assertThat(AvroSchemaUtils.nonNullableSchemaOf(union1)).isSameAs(nonUnionSchema);

        Schema union2 = Schema.createUnion(Schema.create(Schema.Type.NULL), nonUnionSchema);
        assertThat(AvroSchemaUtils.nonNullableSchemaOf(union2)).isSameAs(nonUnionSchema);

        // Unions may contain more non-null types, but the results are undefined and hence not tested.
    }
}
