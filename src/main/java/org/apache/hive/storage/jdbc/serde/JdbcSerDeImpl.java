package org.apache.hive.storage.jdbc.serde;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;

import java.util.List;

/**
 * @ClassName: JdbcSerDeImpl
 * @Author: simo
 * @Date: 2021/6/12 7:59 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public interface JdbcSerDeImpl {


    /**
     * 获取
     * @param obj
     * @param objInspector
     * @param types
     * @param columnNames
     * @return
     * @throws SerDeException
     */
    Writable getSerialize(Object obj, StructObjectInspector objInspector, PrimitiveTypeInfo[] types, List<String> columnNames) throws SerDeException;
}
