package opwvhk.avro.xsd;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static opwvhk.avro.datamodel.Cardinality.MULTIPLE;
import static opwvhk.avro.datamodel.Cardinality.OPTIONAL;
import static opwvhk.avro.datamodel.Cardinality.REQUIRED;
import static opwvhk.avro.datamodel.FixedType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

public class DataRecordsTest {
	@Test
	public void validateFieldDataAsText() {
		Assertions.assertThat(new FieldData("field", null, REQUIRED, null, "abc").toString())
				.isEqualTo("field");
		assertThat(new FieldData("field", "documented", OPTIONAL, STRING, "abc").toString())
				.isEqualTo("field?: string=abc (documented)");
		assertThat(new FieldData("field", "much more documentation", MULTIPLE, STRING, null).toString())
				.isEqualTo("field[]: string (much more…)");
	}

	@Test
	public void validateTypeDataAsText() {
		Assertions.assertThat(new TypeData(null, null, false).toString()).isEqualTo("anonymous");
		assertThat(new TypeData("type", null, true).toString()).isEqualTo("type (mixed)");
		assertThat(new TypeData("type", "something", true).toString()).isEqualTo("type (mixed; something)");
		assertThat(new TypeData("type", "much more text", false).toString()).isEqualTo("type (much more…)");
	}
}
