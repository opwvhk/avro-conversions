Avro Tools
==========

These Avro tools provide means to manipulate and/or describe schemas, including converting XML Schema definitions (XSD) into Avro schemas. Additionally, it provides a way to parse non-Avro data into Avro data structures.

Usage
-----

The class below describes how to use all functionality. As such, it is a contrived example, as (for example) describing a schema in Markdown format and parsing data are not usually combined...

```java
import opwvhk.avro.SchemaManipulator;
import opwvhk.avro.xml.XmlAsAvroParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

class Example {
    public static void main(String[] args) throws Exception {
        Schema fromAvsc = SchemaManipulator.fromAvro(new URL(args[0]))
                // By default, renaming fields/schemata will also add the old name as an alias.
                .renameWithoutAliases()
                .renameField("newName", "path", "to", "field")
                .renameSchema("NewName", "OldName")
                .finish();

        StringBuilder buffer = new StringBuilder();
        Schema fromXsd = SchemaManipulator.fromXsd(new URL(args[1]))
                // Unwrapping drops the single-field schema and makes the schema of its field the schema of the wrapping field.
                .unwrapArray("path", "to", "non-array", "field", "with", "single-element", "schema")
                // Unwraps all non-array fields with single-array-field schemata,
                // provided the field names (except up to the last 3 characters) are the same.
                // Note: 3 is a good number for English names
                .unwrapArrays(3)
                .renameField("newName", "path", "to", "field")
                .renameSchema("NewName", "OldName")
                .alsoDocumentAsMarkdownTable(buffer)
                .finish();

        // Print the schema as a Markdown table
        System.out.println(buffer);

        XmlAsAvroParser parser = new XmlAsAvroParser(new URL(args[1]), args[2], fromXsd, GenericData.get());
        // The record type depends on the class generated by GenericData.get() (you can also use SpecificData or ReflectiveData).
        GenericRecord record = parser.parse(new URL(args[3]));
        System.out.println(record);
    }
}
```




