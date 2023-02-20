package opwvhk.avro.io;

import java.util.Collection;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.FastReaderBuilder;

@SuppressWarnings("rawtypes")
public class DefaultData
		extends GenericData {
	private final GenericData delegate;

	public DefaultData(GenericData delegate) {
		this.delegate = delegate;
	}

	@Override
	public ClassLoader getClassLoader() {
		return delegate.getClassLoader();
	}

	@Override
	public Collection<Conversion<?>> getConversions() {
		return delegate.getConversions();
	}

	@Override
	public void addLogicalTypeConversion(Conversion<?> conversion) {
		delegate.addLogicalTypeConversion(conversion);
	}

	@Override
	public <T> Conversion<T> getConversionByClass(Class<T> datumClass) {
		return delegate.getConversionByClass(datumClass);
	}

	@Override
	public <T> Conversion<T> getConversionByClass(Class<T> datumClass, LogicalType logicalType) {
		return delegate.getConversionByClass(datumClass, logicalType);
	}

	@Override
	public Conversion<Object> getConversionFor(LogicalType logicalType) {
		return delegate.getConversionFor(logicalType);
	}

	@Override
	public GenericData setFastReaderEnabled(boolean flag) {
		return delegate.setFastReaderEnabled(flag);
	}

	@Override
	public boolean isFastReaderEnabled() {
		return delegate.isFastReaderEnabled();
	}

	@Override
	public FastReaderBuilder getFastReaderBuilder() {
		return delegate.getFastReaderBuilder();
	}

	@Override
	public DatumReader createDatumReader(Schema schema) {
		return delegate.createDatumReader(schema);
	}

	@Override
	public DatumReader createDatumReader(Schema writer, Schema reader) {
		return delegate.createDatumReader(writer, reader);
	}

	public DatumWriter createDatumWriter(Schema schema) {
		return delegate.createDatumWriter(schema);
	}

	@Override
	public boolean validate(Schema schema, Object datum) {
		return delegate.validate(schema, datum);
	}

	@Override
	public String toString(Object datum) {
		return delegate.toString(datum);
	}

	@Override
	public Schema induce(Object datum) {
		return delegate.induce(datum);
	}

	@Override
	public void setField(Object record, String name, int position, Object value) {
		delegate.setField(record, name, position, value);
	}

	@Override
	public Object getField(Object record, String name, int position) {
		return delegate.getField(record, name, position);
	}

	@Override
	public int resolveUnion(Schema union, Object datum) {
		return delegate.resolveUnion(union, datum);
	}

	@Override
	public int hashCode(Object o, Schema s) {
		return delegate.hashCode(o, s);
	}

	@Override
	public int compare(Object o1, Object o2, Schema s) {
		return delegate.compare(o1, o2, s);
	}

	@Override
	public Object getDefaultValue(Schema.Field field) {
		return delegate.getDefaultValue(field);
	}

	@Override
	public <T> T deepCopy(Schema schema, T value) {
		return delegate.deepCopy(schema, value);
	}

	@Override
	public Object createFixed(Object old, Schema schema) {
		return delegate.createFixed(old, schema);
	}

	@Override
	public Object createFixed(Object old, byte[] bytes, Schema schema) {
		return delegate.createFixed(old, bytes, schema);
	}

	@Override
	public Object createEnum(String symbol, Schema schema) {
		return delegate.createEnum(symbol, schema);
	}

	@Override
	public Object newRecord(Object old, Schema schema) {
		Object record = delegate.newRecord(old, schema);
		for (Schema.Field field : schema.getFields()) {
			if (field.hasDefaultValue()) {
				setField(record, field.name(), field.pos(), getDefaultValue(field));
			}
		}
		return record;
	}

	@Override
	public Object createString(Object value) {
		return delegate.createString(value);
	}

	@Override
	public Object newArray(Object old, int size, Schema schema) {
		return delegate.newArray(old, size, schema);
	}

	@Override
	public Object newMap(Object old, int size) {
		return delegate.newMap(old, size);
	}

	@Override
	public InstanceSupplier getNewRecordSupplier(Schema schema) {
		return delegate.getNewRecordSupplier(schema);
	}
}
