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
package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;

import org.apache.hive.storage.jdbc.conf.ConfigConsts;
import org.apache.hive.storage.jdbc.split.ColumnSplitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import javax.sql.DataSource;

import java.math.BigInteger;
import java.sql.*;
import java.util.*;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 * @author simo
 */
public class GenericJdbcDatabaseAccessor extends AbstractDatabaseAccessor {

  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcDatabaseAccessor.class);

  public GenericJdbcDatabaseAccessor(Configuration configuration) {
    super(configuration);
  }


  @Override
  public List<String> getColumnNames() {
      return this.columnNames;
  }

  @Override
  public List<String> getColumnTypes() {
    return this.columnTypes;
  }


  @Override
  public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection();
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String countQuery = "SELECT COUNT(*) FROM (" + sql + ") tmptable";
      LOGGER.info("Query to execute is [{}]", countQuery);

      conn = this.dbcpDataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      }
      else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
      }
    }
    catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to get the number of records", e);
      throw new HiveJdbcDatabaseAccessException(e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }
  }


  @Override
  public JdbcRecordIterator
    getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection();
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String limitQuery = addLimitAndOffsetToQuery(sql, limit, offset);
      LOGGER.info("Query to execute is [{}]", limitQuery);

      conn = getDbcpDataSource().getConnection();
      ps = conn.prepareStatement(limitQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();
      return new JdbcRecordIterator(conn, ps, rs, conf.get(serdeConstants.LIST_COLUMN_TYPES));
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }
  }

  @Override
  public JdbcRecordIterator getRecordIterator(Configuration conf, String columnType, String columnName, Object minValue, Object maxValue) throws HiveJdbcDatabaseAccessException{
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection();
      String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String splitCondSql = addSplitColumnFilterToQuery(sql, columnType, columnName, minValue, maxValue);
      LOGGER.error("-------------------- Query to execute is [{}]", splitCondSql);

      conn = getDbcpDataSource().getConnection();
      ps = conn.prepareStatement(splitCondSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();
      return new JdbcRecordIterator(conn, ps, rs, conf.get(serdeConstants.LIST_COLUMN_TYPES));
    }catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }
  }

  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query string
   * @param sql
   * @param limit
   * @param offset
   * @return
   */
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    }
    else {
      return sql + " {LIMIT " + limit + " OFFSET " + offset + "}";
    }
  }

  protected String addSplitColumnFilterToQuery(String sql, String columnType, String columnName, Object minValue, Object maxValue) {
//    String columnType = columnSplitConfig.getColumnType();
//    String columnName = columnSplitConfig.getColumnName();
//    LOGGER.error("---------------------- split value type: " + columnSplitConfig.getColumnMaxValue().getClass());
    //
    LOGGER.error("---------------------- split value type: " + columnType);
    switch (columnType){
      case "INT":
      case "UInt64":
      case "LONG":
        sql = sql + " WHERE " + columnName + ">=" + minValue + " AND " + columnName +"<" + maxValue;
        break;
      default:
        sql = sql + " WHERE " + columnName + ">='" + minValue + "' AND '" + "<" + maxValue + "'";
        break;
    }
    return sql;
  }


  public Map<String , Object > getSplitColumnInfo(Configuration conf, String splitColumn) throws HiveJdbcDatabaseAccessException{
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    HashMap<String , Object> retMap = new HashMap<>();

    try {
      initializeDatabaseConnection();
      String basicSql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String countQuery = String.format("SELECT min(%s) as %s, max(%s) as %s FROM (%s) tmptable",
              splitColumn, ConfigConsts.MIN_SPLIT_VALUE, splitColumn, ConfigConsts.MAX_SPLIT_VALUE, basicSql);
      LOGGER.error("------------ execute sql to count table : " + countQuery);
      conn = getDbcpDataSource().getConnection();
      ps = conn.prepareStatement(countQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      rs = ps.executeQuery();

      retMap.put(ConfigConsts.COLUMN_SPLIT_TYPE, nameTypeMap.get(splitColumn));
      while (rs.next()) {
        LOGGER.error("---------------------- split value type: " + rs.getObject(1).getClass());
        retMap.put(ConfigConsts.MIN_SPLIT_VALUE, rs.getObject(1));
        retMap.put(ConfigConsts.MAX_SPLIT_VALUE, rs.getObject(2));
      }
      LOGGER.error("------------ execute sql to count table, result: " + retMap);
    }catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }
    return retMap;
  }

  public List<ColumnSplitConfig> getColumnSplitInfo(Configuration conf, String splitColumn, Integer partitionNum) throws HiveJdbcDatabaseAccessException{
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    List<ColumnSplitConfig> retColumnInfoList = new ArrayList<>();
    Object maxSplitValue = null;
    Object minSplitValue = null;

    try {
      initializeDatabaseConnection();
      String basicSql = JdbcStorageConfigManager.getQueryToExecute(conf);
      String countQuery = String.format("SELECT min(%s) as %s, max(%s) as %s FROM (%s) tmptable",
              splitColumn, ConfigConsts.MIN_SPLIT_VALUE, splitColumn, ConfigConsts.MAX_SPLIT_VALUE, basicSql);
      LOGGER.error("------------ execute sql to count table : " + countQuery);
      conn = getDbcpDataSource().getConnection();
      ps = conn.prepareStatement(countQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      rs = ps.executeQuery();
      while (rs.next()) {
        LOGGER.error("---------------------- split value type: " + rs.getObject(1).getClass());
        minSplitValue = rs.getObject(1);
        maxSplitValue = rs.getObject(2);
      }
    }catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
    }

    if(maxSplitValue instanceof java.math.BigInteger){
      BigInteger minValue = (BigInteger)minSplitValue;
      BigInteger maxValue = (BigInteger)maxSplitValue;
      BigInteger diff = maxValue.subtract(minValue);

      long numStep = diff.longValue() / partitionNum;
      for(int i = 0; i < partitionNum; i++){
        long min = minValue.longValue() + (i * numStep);
        long max;
        if(i != (partitionNum - 1)){
          max = min + numStep + 1;
        }else {
          max = maxValue.longValue() + 1;
        }
        ColumnSplitConfig columnSplitConfig = new ColumnSplitConfig(splitColumn, nameTypeMap.get(splitColumn), min, max);
        columnSplitConfig.setNumOfRecords((max - min) + 1);
        retColumnInfoList.add(columnSplitConfig);

      }
    }

    return retColumnInfoList;
  }


  public String getColumnType(Configuration conf, String tableName, String columnName) throws Exception {
      Connection conn = getDbcpDataSource().getConnection();
      DatabaseMetaData meta = conn.getMetaData();
      String catalog = conn.getCatalog();
      ResultSet columns = meta.getColumns(catalog, null, tableName, columnName);
      while (columns.next()) {
        if(columns.getString("COLUMN_NAME").equalsIgnoreCase(columnName))
          return columns.getString("TYPE_NAME");
      }
    return null;
  }

  protected int getFetchSize(Configuration conf) {
    return conf.getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }
}
