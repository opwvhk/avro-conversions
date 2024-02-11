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

The main day-to-day use of this library is to parse records in various formats into Avro. As such,
you won't find a converter for (for example) CSV files: these are container files with multiple
records.

The following formats can be converted to Avro:

| Format             | Parser constructor                                  | Parser class                        |
|--------------------|-----------------------------------------------------|-------------------------------------|
| JSON (with schema) | `JsonAsAvroParser(URI, Schema, GenericData)`        | `opwvhk.avro.json.JsonAsAvroParser` |
| JSON (unvalidated) | `JsonAsAvroParser(Schema, GenericData)`             | `opwvhk.avro.json.JsonAsAvroParser` |
| XML (with XSD)     | `XmlAsAvroParser(URL, String, Schema, GenericData)` | `opwvhk.avro.xml.XmlAsAvroParser`   |
| XML (unvalidated)  | `XmlAsAvroParser(Schema, GenericData)`              | `opwvhk.avro.xml.XmlAsAvroParser`   |

Parsers all use both a write schema and a read schema, just like Avro does. The write schema is used
to validate the input, and the read schema is used to describe the result.

When parsing/converting data, the conversion can do implicit conversions that "fit". This includes
like widening conversions (like int→long), lossy conversions (like decimal→float or anything→string)
and parsing dates. With a write schema, binary conversions (from hexadecimal/base64 encoded text)
are also supported.


### Source schema optional but encouraged

The parsers support as much functionality as possible when the write (source) schema is omitted.
However, this is discouraged. The reason is that significant functionality is missing:

* No check on required fields:
  The parsers will happily generate incomplete records, which **will** break when using them.
* No input validation:
  Without a schema, a parser cannot validate input. This can cause unpredictable failures later on.

Summary: you should always use a write (source) schema whenever possible.


### Supported conversions

When parsing, these Avro types are supported:

| Avro                    | JSON (schema) | JSON | XML (schema) | XML |
|-------------------------|---------------|------|--------------|-----|
| Record                  | ✅             | ✅    | ✅            | ✅   |
| Map                     | ❌             | ❌    | ❌            | ❌   |
| Array                   | ✅             | ✅    | ✅            | ✅   |
| Enum                    | ✅             | ✅    | ✅            | ✅   |
| Boolean                 | ✅             | ✅    | ✅            | ✅   |
| Integer                 | ✅             | ✅    | ✅            | ✅   |
| Long                    | ✅             | ✅    | ✅            | ✅   |
| Float                   | ✅             | ✅    | ✅            | ✅   |
| Double                  | ✅             | ✅    | ✅            | ✅   |
| String                  | ✅             | ✅    | ✅            | ✅   |
| Fixed (hex)             | ❌             | ❌    | ❌            | ❌   |
| Fixed (base64)          | ❌             | ❌    | ❌            | ❌   |
| Bytes (hex)             | ✅             | ❌    | ✅            | ❌   |
| Bytes (base64)          | ✅             | ❌    | ✅            | ❌   |
| Decimal                 | ✅             | ✅    | ✅            | ✅   |
| Datetime (millis)       | ✅             | ✅    | ✅            | ✅   |
| Datetime (micros)       | ✅             | ✅    | ✅            | ✅   |
| Local Datetime (millis) | ✅             | ✅    | ✅            | ✅   |
| Local Datetime (micros) | ✅             | ✅    | ✅            | ✅   |
| Date                    | ✅             | ✅    | ✅            | ✅   |
| Time (millis)           | ✅             | ✅    | ✅            | ✅   |
| Time (micros)           | ✅             | ✅    | ✅            | ✅   |



Schema manipulations
--------------------

The class to convert schemas into Avro schemas and other manipulations is
`opwvhk.avro.SchemaManipulator`.

There are multiple starting points:
* `startFromAvro(String)` to parse an Avro schema from a String
* `startFromAvro(URL)` to read an Avro schema from a location
* `startFromJsonSchema(URL)` to read a JSON schema and convert it into an Avro schema
* `startFromXsd(URL)` to read an XML Schema Definition and convert it into an Avro schema

Next, you can rename schemas and fields or unwrap arrays (especially useful for XML). See the
various methods on the class for details.

Note that by default, any rename also adds the previous name as an alias. This allows you to use
the same source schema (be it a JSON schema or XSD) as input for both the parser and schema
manipulation. The advantage is that this causes fields and schemata to be *renamed while parsing*.

And finally, you can document the schema in a Markdown table. This can be your goal (using
`asMarkdownTable()`) or a by-product (using `alsoDocumentAsMarkdownTable(StringBuilder)` and
`finish()`).
