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

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.split.ColumnSplitConfig;

import java.util.List;
import java.util.Map;

public interface DatabaseAccessor {

  List<String> getColumnNames();

  List<String> getColumnTypes();

  int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException;

  JdbcRecordIterator getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException;

//  JdbcRecordIterator getRecordIterator(Configuration conf, ColumnSplitConfig columnSplitConfig) throws HiveJdbcDatabaseAccessException;

  JdbcRecordIterator getRecordIterator(Configuration conf, String columnType, String columnName, Object minValue, Object maxValue) throws HiveJdbcDatabaseAccessException;

  Map<String , Object > getSplitColumnInfo(Configuration conf, String splitColumn) throws HiveJdbcDatabaseAccessException;

  List<ColumnSplitConfig> getColumnSplitInfo(Configuration conf, String splitColumn, Integer partitionNum) throws HiveJdbcDatabaseAccessException;
}
