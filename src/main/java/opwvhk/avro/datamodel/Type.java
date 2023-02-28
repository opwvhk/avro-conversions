package opwvhk.avro.datamodel;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import static java.util.Collections.emptyList;
import static opwvhk.avro.datamodel.StructType.Field.NULL_VALUE;
import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;
import static org.apache.avro.Schema.Type.*;

public sealed interface Type permits ScalarType, StructType, TypeWithUnparsedContent {
	static Type fromSchema(Schema schema) {
		return fromSchema(new TypeCollection(), schema);
	}

	private static Type fromSchema(TypeCollection typeCollection, Schema schema) {
		if (schema.getType() == RECORD) {
			return fromRecordSchema(typeCollection, schema);
		}

		LogicalType logicalType = schema.getLogicalType();
		if (logicalType != null) {
			if (logicalType instanceof LogicalTypes.Date) {
				return FixedType.DATE;
			} else if (logicalType instanceof LogicalTypes.TimeMillis) {
				return FixedType.TIME;
			} else if (logicalType instanceof LogicalTypes.TimeMicros) {
				return FixedType.TIME_MICROS;
			} else if (logicalType instanceof LogicalTypes.TimestampMillis) {
				return FixedType.DATETIME;
			} else if (logicalType instanceof LogicalTypes.TimestampMicros) {
				return FixedType.DATETIME_MICROS;
			} else if (logicalType instanceof LogicalTypes.Decimal decimal) {
				int scale = decimal.getScale();
				int precision = decimal.getPrecision();
				if (scale == 0) {
					int bitSize = BigDecimal.TEN.pow(precision).toBigInteger().bitLength();
					return DecimalType.integer(bitSize, precision);
				} else {
					return DecimalType.withFraction(precision, scale);
				}
			}
		}
		return switch (schema.getType()) {
			case BOOLEAN -> FixedType.BOOLEAN;
			case INT -> DecimalType.INTEGER_TYPE;
			case LONG -> DecimalType.LONG_TYPE;
			case STRING -> FixedType.STRING;
			case BYTES -> FixedType.BINARY_HEX; // Any binary type will do: we're not parsing.
			case DOUBLE -> FixedType.DOUBLE;
			case FLOAT -> FixedType.FLOAT;
			case ENUM -> new EnumType(typeCollection, schema.getFullName(), schema.getAliases(), schema.getDoc(), schema.getEnumSymbols(), schema.getEnumDefault());
			default -> throw new IllegalArgumentException("Unsupported schema type %s in: %s".formatted(schema.getType(), schema));
		};
	}

	private static StructType fromRecordSchema(TypeCollection typeCollection, Schema schema) {
		StructType existingType = (StructType) typeCollection.getType(schema.getFullName());
		if (existingType != null) {
			return existingType;
		}

		StructType structType = new StructType(typeCollection, schema.getFullName(), schema.getAliases(), schema.getDoc());

		List<Schema.Field> schemaFields = schema.getFields();
		List<StructType.Field> fields = new ArrayList<>(schemaFields.size());
		for (Schema.Field schemaField : schemaFields) {
			Cardinality cardinality = Cardinality.REQUIRED;
			Schema fieldSchema = schemaField.schema();
			Object defaultValue = schemaField.defaultVal();
			while (true) {
				if (fieldSchema.getType() == ARRAY) {
					cardinality = cardinality.adjustFor(Cardinality.MULTIPLE);
					fieldSchema = fieldSchema.getElementType();
				} else if (fieldSchema.getType() == UNION) {
					List<Schema> types = fieldSchema.getTypes();
					int unionSize = 1;
					if (fieldSchema.isNullable()) {
						cardinality = cardinality.adjustFor(Cardinality.OPTIONAL);
						unionSize = 2;
					}
					if (types.size() != unionSize) {
						throw new IllegalArgumentException("Unsupported union: only unions of null and one other schema are supported.");
					}
					Schema schema0 = types.get(0);
					fieldSchema = schema0.isNullable() ? types.get(1) : schema0;
				} else {
					break;
				}
			}
			if (cardinality == Cardinality.MULTIPLE) {
				defaultValue = emptyList();
			}
			Type fieldType = fromSchema(typeCollection, fieldSchema);
			fields.add(new StructType.Field(schemaField.name(), schemaField.aliases(), schemaField.doc(), cardinality, fieldType, defaultValue));
		}

		structType.setFields(fields);
		return structType;
	}

	default Schema toSchema() {
		if (this instanceof StructType structType) {
			List<Schema.Field> fields = structType.fields().stream().map(f -> {
				Schema fieldSchema = f.type().toSchema();
				Object defaultValue = f.defaultValue();
				if (f.cardinality() == Cardinality.MULTIPLE) {
					fieldSchema = Schema.createArray(fieldSchema);
				} else if (f.cardinality() == Cardinality.OPTIONAL) {
					if (defaultValue != NULL_VALUE && defaultValue != JsonProperties.NULL_VALUE && defaultValue != NULL_DEFAULT_VALUE) {
						fieldSchema = Schema. createUnion(fieldSchema, Schema.create(NULL));
					} else {
						fieldSchema = Schema. createUnion(Schema.create(NULL), fieldSchema);
					}
				}
				Schema.Field field = new Schema.Field(f.name(), fieldSchema, f.documentation(), defaultValue == NULL_VALUE ? NULL_DEFAULT_VALUE : defaultValue);
				f.aliases().forEach(field::addAlias);
				return field;
			}).toList();
			Schema schema = Schema.createRecord(structType.name(), structType.documentation(), null, false, fields);
			structType.aliases().forEach(schema::addAlias);
			return schema;
		} else if (this instanceof EnumType enumType) {
			Schema schema = Schema.createEnum(enumType.name(), enumType.documentation(), null, enumType.enumSymbols());
			enumType.aliases().forEach(schema::addAlias);
			return schema;
		} else if (this instanceof DecimalType decimalType) {
			if (decimalType.scale() == 0 && decimalType.bitSize() <= Integer.SIZE) {
				return Schema.create(INT);
			} else if (decimalType.scale() == 0 && decimalType.bitSize() <= Long.SIZE) {
				return Schema.create(LONG);
			} else {
				return LogicalTypes.decimal(decimalType.precision(), decimalType.scale()).addToSchema(Schema.create(BYTES));
			}
		} else {
			FixedType fixedType = (FixedType) this;
			return switch (fixedType) {
				case BOOLEAN -> Schema.create(BOOLEAN);
				case FLOAT -> Schema.create(FLOAT);
				case DOUBLE -> Schema.create(DOUBLE);
				case DATE -> LogicalTypes.date().addToSchema(Schema.create(INT));
				case DATETIME -> LogicalTypes.timestampMillis().addToSchema(Schema.create(LONG));
				case DATETIME_MICROS -> LogicalTypes.timestampMicros().addToSchema(Schema.create(LONG));
				case TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(INT));
				case TIME_MICROS -> LogicalTypes.timeMicros().addToSchema(Schema.create(LONG));
				case BINARY_HEX, BINARY_BASE64 -> Schema.create(BYTES);
				default /* STRING */ -> Schema.create(STRING);
			};
		}
	}

	String debugString(String indent);
}
