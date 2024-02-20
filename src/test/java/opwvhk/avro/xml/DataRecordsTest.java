package opwvhk.avro.xml;

import org.junit.jupiter.api.Test;

import static opwvhk.avro.xml.datamodel.Cardinality.MULTIPLE;
import static opwvhk.avro.xml.datamodel.Cardinality.OPTIONAL;
import static opwvhk.avro.xml.datamodel.Cardinality.REQUIRED;
import static opwvhk.avro.xml.datamodel.FixedType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

class DataRecordsTest {
    @Test
    void validateFieldDataAsText() {
        assertThat(new FieldData("field", null, REQUIRED, null, "abc").toString())
                .isEqualTo("field");
        assertThat(new FieldData("field", "documented", OPTIONAL, STRING, "abc").toString())
                .isEqualTo("field?: string=abc (documented)");
        assertThat(new FieldData("field", "much more documentation", MULTIPLE, STRING, null).toString())
                .isEqualTo("field[]: string (much more…)");
    }

    @Test
    void validateTypeDataAsText() {
        assertThat(new TypeData(null, null, false).toString()).isEqualTo("anonymous");
        assertThat(new TypeData("type", null, true).toString()).isEqualTo("type (mixed)");
        assertThat(new TypeData("type", "something", true).toString()).isEqualTo("type (mixed; something)");
        assertThat(new TypeData("type", "much more text", false).toString()).isEqualTo("type (much more…)");
    }
}
