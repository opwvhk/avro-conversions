package opwvhk.avro.util;

import java.io.StringReader;

import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
				| textOrNumber | int | This is an unlikely field, containing either text or a number. |""";

		Schema schema = new Idl(new StringReader(idlSchema)).CompilationUnit().getType("MainRecord");
		String result = AvroSchemaUtils.documentAsMarkdown(schema);

		assertThat(result).isEqualTo(expectedResult);
	}

	@Test
	public void checkDocumentationNewlinesAreHtml() {
		String textWithNewlines = "Line 1\nLine 2\nLine 3";
		String textWithHtmlLineBreaks = "Line 1<br/>Line 2<br/>Line 3";
		AvroSchemaUtils.Entry entry = new AvroSchemaUtils.Entry("", "", textWithNewlines);
		assertThat(entry.documentation()).isEqualTo(textWithNewlines);
		assertThat(entry.docForMDTableCell()).isEqualTo(textWithHtmlLineBreaks);
	}

	@Test
	public void testAvroSchemaFieldSorting() throws ParseException {
		Schema schema = new Idl(new StringReader("""
				protocol dummy {
					record MainRecord {
						string name;
						string? description;
						map<string> properties = {};
						array<string> aliases = [];
						MainRecord loop;
					}
				}""")).CompilationUnit().getType("MainRecord");
		Schema expected = new Idl(new StringReader("""
				protocol dummy {
					record MainRecord {
						array<string> aliases = [];
						string? description;
						MainRecord loop;
						string name;
						map<string> properties = {};
					}
				}""")).CompilationUnit().getType("MainRecord");
		assertThat(schema).isNotEqualTo(expected);
		assertThat(AvroSchemaUtils.sortFields(schema)).isEqualTo(expected);
	};
}
