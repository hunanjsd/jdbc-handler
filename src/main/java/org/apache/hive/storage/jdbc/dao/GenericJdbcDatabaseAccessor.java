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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import javax.sql.DataSource;

import java.sql.*;
import java.util.*;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 * @author simo
 */
public class GenericJdbcDatabaseAccessor extends AbstractDatabaseAccessor {

  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcDatabaseAccessor.class);
  protected DataSource dbcpDataSource = null;

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

      conn = dbcpDataSource.getConnection();
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


  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query string
   *
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


  protected int getFetchSize(Configuration conf) {
    return conf.getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }
}
