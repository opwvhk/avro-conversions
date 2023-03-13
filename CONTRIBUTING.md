Contributing
============

So you want/need to understand the code, and possibly contribute? Welcome!

This document describes the high-level requirements, code layout and the major design choices.


High-level Requirements
-----------------------

This library is mainly intended to parse data into Avro records. Initial use cases are XML and JSON parsing. Some auxiliary functionality required to understand
the data and/or for data governance is also included. An obvious choice is generating an Avro schema from an XSD, but generating a (markdown) table from a
schema is also included because it is so massively useful when dealing with data governance.

To ensure the main functionality of parsing data into Avro records, these are the requirements for the design:
* All data must be compatible with Avro, Parquet and easily understood data structures (this also means compatibility with plain objects)
* When reading data, an equivalent of Avro schema resolution must be applied
* For XML, wrapped arrays should be parsable as plain arrays as well
* Any data that can be supported should be supported.
* For structural types, this means that objects and arrays are defined to cover all possibilities (where `xs:choice`, `oneOf` and the like generate optional properties).
* For JSON, the following scalar types are supported:
  * All basics: `enum`/`const`, and types `string`, `integer`, `number`, `boolean`, with `string` also allowing some `format` options
  * Enums are supported for string values only
  * Non-integer numbers are interpreted according to configuration. Options are to use fixed point or floating point numbers. The default is to use floating point numbers; fixed point numbers can be used only for numbers with limits (`minimum`, `exclusiveMinimum`, `maximum` and `exclusiveMaximum`). Floating point numbers result in an Avro `float`, unless the limits are larger than &plusmn;2<sup>60</sup> or smaller than &plusmn;2<sup>-60</sup> (these values fairly arbitrary, but ensure parsed values fit comfortably).
  * Strings are treated as string unless a supported format property is present. The formats `date`, `date-time` and `time` are parsed according to ISO 8601.
* For XML, the following scalar types are supported:
  * Supported are:
    `anyURI`, `base64Binary`, `boolean`, `byte`, `date`, `dateTime`, `decimal`, `double`, `ENTITY`, `float`, `hexBinary`, `ID`, `IDREF`, `int`,
    `integer`, `language`, `long`, `Name`, `NCName`, `negativeInteger`, `NMTOKEN`, `nonNegativeInteger`, `nonPositiveInteger`, `normalizedString`,
    `positiveInteger`, `short`, `string`, `time`, `token`, `unsignedByte`, `unsignedInt`, `unsignedLong`, `unsignedShort`
  * Not supported are:
    `duration` (because it mixes properties of JVM `java.time.Duration` and `java.time.Period`),
    `gYear`, `gYearMonth`, `gDay`, `gMonth`, `gMonthDay` (because they don't have a standard representation in Avro),
    `NOTATION`, `QName` (because they're complex structures with two fields), and
    `ENTITIES`, `IDREFS`, `NMTOKENS` (because they're lists)
* For all formats:
  * Limitless integer numbers are coerced to Avro `long`, to maximise use of primitive types (in Avro, decimal types are logical types on a byte array).
  * Formats with a time component are tricky: Avro does not support a timezone in the data itself. Therefore, times are stored in UTC. Also, times are parsed with optional timezone, defaulting to UTC during parsing.


Code Layout
-----------

This section is not needed to use Avro Tools, but useful if you're tasked with maintenance.
The information here is very succinct, in the hope it won't become outdated quickly.

Initially, you'll find these packages:
* `opwvhk.avro`
* `opwvhk.avro.json`
* `opwvhk.avro.xml`
* `opwvhk.avro.xml.datamodel`
* `opwvhk.avro.util`

The package `opwvhk.avro` it the entry point. The subpackage `util` contains various utilities and is not very useful by itself.

The subpackage `xml` contains all code related to XSD and XML parsing, with a further subpackage `datamodel`.

The subpackage `json` contains all code related to JSON Schema and JSON parsing.


XSD Data Model
--------------

Two of the requirements are schema resolution, including unwrapping arrays, and reading binary data (in XML encoded as hexadecimal or base64).
To enable schema resolution, one needs to build a resolver tree to parse XML data. For these to unwrap arrays, one must look ahead to see if one is about to
parse a record with a single array field. This lookahead means that you cannot build resolvers while parsing the XSD.

But adding a property in the Avro schema that says if the binary data is hex or base64 encoded in the XML is not a clean option: Avro properties describe the
Avro data, not the XML.

To provide a data structure for this lookahead, the package `opwvhk.avro.xml.datamodel` contains type descriptions that describe XML schemas as objects with
properties.
