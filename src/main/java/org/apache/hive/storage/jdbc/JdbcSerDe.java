/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.serde.JdbcSerDeImpl;
import org.apache.hive.storage.jdbc.serde.JdbcSerDeImplFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;

import java.util.*;

public class JdbcSerDe extends AbstractSerDe {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSerDe.class);

  private StructObjectInspector objectInspector;
  private int numColumns;
  private String[] hiveColumnTypeArray;
  private PrimitiveTypeInfo[] types;
  private List<String> columnNames;
  private List<Object> row;
  private Configuration tableConfig;
  private Properties tableProperties;


  /**
   * This method gets called multiple times by Hive. On some invocations, the properties will be empty.
   *  We need to detect when the properties are not empty to initialise the class variables.
   *  @see org.apache.hadoop.hive.serde2.Deserializer#initialize(org.apache.hadoop.conf.Configuration, java.util.Properties)
   * @param conf
   * @param tbl
   * @throws SerDeException
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    try {
      LOGGER.trace("Initializing the SerDe");
      this.tableProperties = tbl;
      if (this.tableProperties.containsKey(JdbcStorageConfig.DATABASE_TYPE.getPropertyName())) {

        this.tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(tbl);
        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);
        columnNames = dbAccessor.getColumnNames();
        numColumns = columnNames.size();
        List<String> hiveColumnNames;

        String[] hiveColumnNameArray = parseProperty(tbl.getProperty(serdeConstants.LIST_COLUMNS), ",");
        if (numColumns != hiveColumnNameArray.length) {
          throw new SerDeException("Expected " + numColumns + " columns. Table definition has "
              + hiveColumnNameArray.length + " columns");
        }
        hiveColumnNames = Arrays.asList(hiveColumnNameArray);

        hiveColumnTypeArray = parseProperty(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES), ":");
        if (hiveColumnTypeArray.length == 0) {
          throw new SerDeException("Received an empty Hive column type definition");
        }

        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(numColumns);
        List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
          PrimitiveTypeInfo ti = TypeInfoFactory.getPrimitiveTypeInfo(hiveColumnTypeArray[i]);
          ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(ti);
          columnTypes.add(ti);
          fieldInspectors.add(oi);
        }
        types =  columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
        objectInspector =
          ObjectInspectorFactory.getStandardStructObjectInspector(hiveColumnNames,
              fieldInspectors);
        row = new ArrayList<>(numColumns);
      }
    }catch (Exception e) {
      LOGGER.error("Caught exception while initializing the SqlSerDe", e);
      throw new SerDeException(e);
    }
  }


  private String[] parseProperty(String propertyValue, String delimiter) {
    if ((propertyValue == null) || (propertyValue.trim().isEmpty())) {
      return new String[] {};
    }

    return propertyValue.split(delimiter);
  }


  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    LOGGER.trace("Deserializing from SerDe");
    if (!(blob instanceof MapWritable)) {
      throw new SerDeException("Expected MapWritable. Got " + blob.getClass().getName());
    }

    if ((row == null) || (columnNames == null)) {
      throw new SerDeException("JDBC SerDe hasn't been initialized properly");
    }

    row.clear();
    MapWritable input = (MapWritable) blob;
    Text columnKey = new Text();

    for (int i = 0; i < numColumns; i++) {
      columnKey.set(columnNames.get(i));
      Writable value = input.get(columnKey);
      row.add(value instanceof NullWritable ? null : ((ObjectWritable)value).get());
    }

    return row;
  }


  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }


  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }


  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString() + " can only serialize struct types, but we got: "
              + objectInspector.getTypeName());
    }

    DatabaseType dbType = DatabaseType.valueOf(this.tableProperties.getProperty(JdbcStorageConfig.DATABASE_TYPE.getPropertyName()));
    JdbcSerDeImpl jdbcSerDe = JdbcSerDeImplFactory.getJdbcSerDeImpl(dbType);
    return jdbcSerDe.getSerialize(obj, objectInspector, types, columnNames);
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

}
