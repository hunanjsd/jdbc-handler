package org.apache.hive.storage.jdbc.serde;

import com.jcraft.jsch.Logger;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.lazy.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.slf4j.LoggerFactory;

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

    private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ClickhouseJdbcSerDeImpl.class);

    @Override
    public Writable getSerialize(Object obj, StructObjectInspector objInspector, PrimitiveTypeInfo[] types, List<String> columnNames) throws SerDeException {
        List<? extends StructField> fields = objInspector.getAllStructFieldRefs();
        List<Object> values = objInspector.getStructFieldsDataAsList(obj);

        final Map<String, Object> value = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            Object columnValue = values.get(i);
            if (columnValue == null) {
                // null, we just add it
                value.put(columnNames.get(i), null);
                continue;
            }

//            LOGGER.error(String.format("column name: %s,fields.get(i).getFieldObjectInspector(): %s, column value: %s, types[i].getPrimitiveCategory(): %s, types i: %s, fields i: %s", columnNames.get(i), fields.get(i).getFieldObjectInspector().getClass(), columnValue.getClass(), types[i].getPrimitiveCategory().getClass(), types[i].getClass(), fields.get(i).getClass()));
            final Object res;
            switch (types[i].getPrimitiveCategory()) {
                case TIMESTAMP:
                    if (columnValue instanceof LazyTimestamp) {
                        res = ((LazyTimestamp) columnValue).getWritableObject().getTimestamp();
                    }else if(columnValue instanceof TimestampWritable){
                        res = ((TimestampWritable) columnValue).getTimestamp();
                    }else {
                        res = ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i));
                    }
                    break;
                case BYTE:
                    if (columnValue instanceof LazyByte) {
                        res = (int) ((LazyByte) columnValue).getWritableObject().get();
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.ByteWritable){
                        res = ((ByteWritable) columnValue).get();
                    } else {
                        res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case SHORT:
                    if (columnValue instanceof LazyShort) {
                        res = ((LazyShort) columnValue).getWritableObject().get();
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.ShortWritable){
                        res = ((ShortWritable) columnValue).get();
                    } else {
                        res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case INT:
                    if (columnValue instanceof LazyInteger) {
                        res = ((LazyInteger) columnValue).getWritableObject().get();
                    }else {
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
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.DoubleWritable){
                        res = ((DoubleWritable) columnValue).get();
                    } else {
                        res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                    }
                    break;
                case CHAR:
                    if (columnValue instanceof LazyHiveChar) {
                        res = ((LazyHiveChar) columnValue).getWritableObject().toString();
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.HiveCharWritable){
                        res = ((HiveCharWritable) columnValue).getHiveChar();
                    } else {
                        res = ((HiveCharObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i)).getValue();
                    }
                    break;
                case VARCHAR:
                    if (columnValue instanceof LazyHiveVarchar) {
                        res = ((LazyHiveVarchar) columnValue).getWritableObject().toString();
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.HiveVarcharWritable){
                        res = ((HiveVarcharWritable) columnValue).getHiveVarchar();
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
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.DateWritable){
                        res = ((DateWritable) columnValue).get();
                    } else {
                        res = ((DateObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i));
                    }
                    break;
                case DECIMAL:
                    if (columnValue instanceof org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal) {
                        res = ((LazyHiveDecimal) columnValue).getWritableObject().getHiveDecimal().bigDecimalValue();
                    }else if(columnValue instanceof org.apache.hadoop.hive.serde2.io.HiveDecimalWritable){
                        res = ((HiveDecimalWritable) columnValue).getHiveDecimal().bigDecimalValue();
                    } else {
                        res = ((org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector) fields.get(i).getFieldObjectInspector())
                                .getPrimitiveJavaObject(values.get(i)).bigDecimalValue();
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
