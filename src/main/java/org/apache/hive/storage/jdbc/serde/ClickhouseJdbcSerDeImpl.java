package org.apache.hive.storage.jdbc.serde;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: ClickhouseJdbcSerDeImpl
 * @Author: simo
 * @Date: 2021/6/12 8:04 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public class ClickhouseJdbcSerDeImpl implements JdbcSerDeImpl{

    @Override
    public Writable getSerialize(Object obj, StructObjectInspector objInspector, PrimitiveTypeInfo[] types, List<String> columnNames) throws SerDeException {
        StructObjectInspector soi = objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> values = soi.getStructFieldsDataAsList(obj);

        final Map<String, Object> value = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            Object columnValue = values.get(i);
            if (columnValue == null) {
                // null, we just add it
                value.put(columnNames.get(i), null);
                continue;
            }
            final Object res;
            switch (types[i].getPrimitiveCategory()) {
                case TIMESTAMP:
                    if (columnValue instanceof LazyTimestamp) {
                        res = ((LazyTimestamp) columnValue).getWritableObject().getTimestamp();
                    } else {
                        res = ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i));
                    }
                    break;
                case BYTE:
                    if (columnValue instanceof LazyByte) {
                        res = (int) ((LazyByte) columnValue).getWritableObject().get();
                    } else {
                        res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case SHORT:
                    if (columnValue instanceof LazyShort) {
                        res = ((LazyShort) columnValue).getWritableObject().get();
                    } else {
                        res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case INT:
                    if (columnValue instanceof LazyInteger) {
                        res = ((LazyInteger) columnValue).getWritableObject().get();
                    } else {
                        res = ((IntObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case LONG:
                    if (columnValue instanceof LazyLong) {
                        res = ((LazyLong) columnValue).getWritableObject().get();
                    } else {
                        res = ((LongObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case FLOAT:
                    if (columnValue instanceof LazyFloat) {
                        res = ((LazyFloat) columnValue).getWritableObject().get();
                    } else {
                        res = ((FloatObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case DOUBLE:
                    if (columnValue instanceof LazyDouble) {
                        res = ((LazyDouble) columnValue).getWritableObject().get();
                    } else {
                        res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case CHAR:
                    if (columnValue instanceof LazyHiveChar) {
                        res = ((LazyHiveChar) columnValue).getWritableObject().toString();
                    } else {
                        res = ((HiveCharObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i)).getValue();
                    }
                    break;
                case VARCHAR:
                    if (columnValue instanceof LazyHiveVarchar) {
                        res = ((LazyHiveVarchar) columnValue).getWritableObject().toString();
                    } else {
                        res = ((HiveVarcharObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i)).getValue();
                    }
                    break;
                case STRING:
                    if (columnValue instanceof LazyString) {
                        res = ((LazyString) columnValue).getWritableObject().toString();
                    } else {
                        res = ((StringObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i));
                    }
                    break;
                case DATE:
                    if (columnValue instanceof LazyDate) {
                        res = ((LazyDate) columnValue).getWritableObject().get();
                    } else {
                        res = ((DateObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i));
                    }
                    break;
                default:
                    throw new SerDeException("Unsupported type: " + types[i].getPrimitiveCategory());
            }
            value.put(columnNames.get(i), res);
        }
        return new ClickHouseWritable(value);
    }
}
