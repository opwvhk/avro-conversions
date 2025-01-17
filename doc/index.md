Avro Conversions - Documentation
================================

Usage
-----

This library is intended to parse data and to manipulate schemas for that. As such, this
documentation is split into two parts: parsing & schema manipulation.

Want to get started quickly? See the [Quickstart](../README.md#quickstart) section in the README.

This document describes the various functionality in more detail.


Parsing
-------

The main day-to-day use of this library is to parse single records in various formats into Avro. As
a result, you won't find a converter for (for example) CSV files: these are container files with
multiple records.

The following formats can be converted to Avro:

| Format | Parser class                        |
|--------|-------------------------------------|
| JSON   | `opwvhk.avro.json.JsonAsAvroParser` |
| XML    | `opwvhk.avro.xml.XmlAsAvroParser`   | 

Parsers require a read schema and an Avro model, determining the Avro record type to parse data into
and how to create the these records, respectively. Additionally, they support a format dependent
"write schema" (i.e., JSON schema, XSD, &hellip;), which is used for schema validation, and can be
used for input validation.

### Schema evolution

When parsing/converting data, the conversion can do implicit conversions that "fit". This includes
like widening conversions (like int→long), lossy conversions (like decimal→float or anything→string)
and parsing dates. With a write schema, binary conversions (from hexadecimal/base64 encoded text)
are also supported.

In addition, the read schema is used for schema evolution:

* removing fields: fields that are not present in the read schema will be ignored
* adding fields: fields that are not present in the input will be filled with the default values
  from the read schema
* renaming fields: field aliases are also used to match incoming data, effectively renaming these
  fields

### Source schema optional but encouraged

The parsers support as much functionality as possible when the write (source) schema is omitted.
However, this is discouraged. The reason is that significant functionality is missing:

* No check on required fields:
  The parsers will happily generate incomplete records, which **will** break when using them.
* No check on compatibility:
  Incompatible data cannot be detected cleanly, and this **will** break the parsing process in
  unpredictable ways.
* No input validation:
  Without a schema, a parser cannot validate input. This can cause unpredictable failures later on.

Summary: you should always use a write (source) schema whenever possible.

### Supported conversions

When parsing, these Avro types are supported:

| Avro                    | JSON | XML |
|-------------------------|------|-----|
| Record                  | ✅    | ✅   |
| Map                     | ❌    | ❌   |
| Array                   | ✅    | ✅   |
| Enum                    | ✅    | ✅   |
| Boolean                 | ✅    | ✅   |
| Integer                 | ✅    | ✅   |
| Long                    | ✅    | ✅   |
| Float                   | ✅    | ✅   |
| Double                  | ✅    | ✅   |
| String                  | ✅    | ✅   |
| Fixed (base 16)¹        | ✅    | ✅   |
| Fixed (base 64)²        | ✅    | ✅   |
| Bytes (base 16)¹        | ✅    | ✅   |
| Bytes (base 64)²        | ✅    | ✅   |
| Decimal                 | ✅    | ✅   |
| Datetime (millis)       | ✅    | ✅   |
| Datetime (micros)       | ✅    | ✅   |
| Local Datetime (millis) | ✅    | ✅   |
| Local Datetime (micros) | ✅    | ✅   |
| Date                    | ✅    | ✅   |
| Time (millis)           | ✅    | ✅   |
| Time (micros)           | ✅    | ✅   |

Notes:
1. Base 16 encoded bytes require either a source-format schema (like an XSD), or a property "format"
   with the value "base16"
2. Base 64 encoded bytes require either a source-format schema (like an XSD), or a property "format"
   with the value "base64"

Schema manipulations
--------------------

The class to convert schemas into Avro schemas and other manipulations is
`opwvhk.avro.SchemaManipulator`.

There are multiple starting points:

* `startFromAvro(String)` to parse an Avro schema from a String
* `startFromAvro(URL)` to read an Avro schema from a location
* `startFromJsonSchema(URL)` to read a JSON schema and convert it into an Avro schema
* `startFromXsd(URL)` to read an XML Schema Definition and convert it into an Avro schema

Next, you can rename schemas and fields, apply naming conventions, and/or unwrap arrays (especially
useful for XML). See the various methods on the class for details:

| Manipulation           | Method                                                                                                            |
|------------------------|-------------------------------------------------------------------------------------------------------------------|
| Rename single schema   | `renameSchema(String, String)`<br/>`renameSchemaAtPath(String, String...)`                                        |
| Rename all schemas     | `useSchemaNamingConvention(NamingConvention, NamingConvention)`<br/>`useSchemaNamingConvention(NamingConvention)` |
| Rename single field    | `renameField(String, String, String)`<br/>`renameFieldAtPath(String, String...)`                                  |
| Rename all fields      | `useFieldNamingConvention(NamingConvention)`                                                                      |
| Unwrap single array    | `unwrapArray(String, String)`<br/>`unwrapArray(String...)`                                                        |
| Unwrap multiple arrays | `unwrapArrays(int)`                                                                                               |
| Sort fields            | `sortFields()`                                                                                                    |

Note that by default, any rename also adds the previous name as an alias. This allows you to use
the same source schema (be it a JSON schema or XSD) as input for both the parser and schema
manipulation. The advantage is that this causes fields and schemata to be *renamed while parsing*.

And finally, you can document the schema in a Markdown table. This can be your goal (using
`asMarkdownTable()`) or a by-product (using `alsoDocumentAsMarkdownTable(StringBuilder)` and
`finish()`).

