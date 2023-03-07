package opwvhk.avro.xml.datamodel;

import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;

public sealed interface Type permits ScalarType, StructType, TypeWithUnparsedContent {

	default Schema toSchema() {
		if (this instanceof StructType structType) {
			List<Schema.Field> fields = structType.fields().stream().map(f -> {
				Schema fieldSchema = f.type().toSchema();
				Object defaultValue = f.defaultValue();
				if (f.cardinality() == Cardinality.MULTIPLE) {
					fieldSchema = Schema.createArray(fieldSchema);
				} else if (f.cardinality() == Cardinality.OPTIONAL) {
					if (defaultValue != StructType.Field.NULL_VALUE && defaultValue != JsonProperties.NULL_VALUE && defaultValue != NULL_DEFAULT_VALUE) {
						fieldSchema = Schema.createUnion(fieldSchema, Schema.create(NULL));
					} else {
						fieldSchema = Schema.createUnion(Schema.create(NULL), fieldSchema);
					}
				}
				return new Schema.Field(f.name(), fieldSchema, f.documentation(),
						defaultValue == StructType.Field.NULL_VALUE ? NULL_DEFAULT_VALUE : defaultValue);
			}).toList();
			return Schema.createRecord(structType.name(), structType.documentation(), null, false, fields);
		} else if (this instanceof EnumType enumType) {
			return Schema.createEnum(enumType.name(), enumType.documentation(), null, enumType.enumSymbols());
		} else if (this instanceof DecimalType decimalType) {
			if (decimalType.bitSize() <= Integer.SIZE) {
				return Schema.create(INT);
			} else if (decimalType.bitSize() <= Long.SIZE) {
				return Schema.create(LONG);
			} else {
				return LogicalTypes.decimal(decimalType.precision(), decimalType.scale()).addToSchema(Schema.create(BYTES));
			}
		} else if (this instanceof FixedType fixedType) {
			return switch (fixedType) {
				case BOOLEAN -> Schema.create(BOOLEAN);
				case FLOAT -> Schema.create(FLOAT);
				case DOUBLE -> Schema.create(DOUBLE);
				case DATE -> LogicalTypes.date().addToSchema(Schema.create(INT));
				// TODO: Choose between millis and micros
				case DATETIME -> LogicalTypes.timestampMillis().addToSchema(Schema.create(LONG));
				case TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(INT));
				case BINARY_HEX, BINARY_BASE64 -> Schema.create(BYTES);
				default /* STRING */ -> Schema.create(STRING);
			};
		} else {
			return ((TypeWithUnparsedContent)this).actualType().toSchema();
		}
	}

	String debugString(String indent);
}
