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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.storage.jdbc.clickhouse.ClickHouseHelper;
import org.apache.hive.storage.jdbc.clickhouse.ClickHouseOutputFormat;
import org.apache.hive.storage.jdbc.clickhouse.ClickHouseRecordWriter;
import org.apache.hive.storage.jdbc.clickhouse.Constants;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JdbcOutputFormat implements OutputFormat<NullWritable, MapWritable>,
                                         HiveOutputFormat<NullWritable, MapWritable> {
  private static final Logger logger = LoggerFactory.getLogger(ClickHouseOutputFormat.class);
  /**
   * {@inheritDoc}
   */
  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tblProps,
      Progressable progress) throws IOException {

    logger.info("Table Properties");
    Set<Map.Entry<Object, Object>> entries = tblProps.entrySet();
    for (Map.Entry<Object, Object> entry: entries) {
      logger.info(entry.getKey() + " : " + entry.getValue());
    }

    String ckConnectionStrings = tblProps.getProperty(JdbcStorageConfig.JDBC_URL.getPropertyName());
    String tblName = tblProps.getProperty(JdbcStorageConfig.TABLE.getPropertyName());
    // Table name and connection string are required
    if (ckConnectionStrings == null || ckConnectionStrings == "") {
      throw new IOException(JdbcStorageConfig.JDBC_URL.getPropertyName() + " must be set in TBLPROPERTIES");
    }

    if (tblName == null || tblName == "") {
      throw new IOException(JdbcStorageConfig.TABLE.getPropertyName() + " must be set in TBLPROPERTIES");
    }

    String batchSizeStr = tblProps.getProperty(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName());
    int batchSize = 0;
    try {
      if (batchSizeStr == null || batchSizeStr == "") {
        batchSize = Constants.DEFAULT_BATCH_SIZE;
      } else {
        batchSize = Integer.parseInt(batchSizeStr);
      }
    } catch (NumberFormatException e) {
      logger.info(String.format("Parsing %s failed, use default", batchSizeStr), e);
      batchSize = Constants.DEFAULT_BATCH_SIZE;
    }

    ClickHouseHelper ckHelper;
    try {
      ckHelper = ClickHouseHelper.getClickHouseHelper(ckConnectionStrings, tblName);
    } catch (SQLException e) {
      logger.error("Can't create ckHelper ", e);
      throw new IOException(e);
    }
    List<String> columnNames = ckHelper.getColumnNames();
    List<String> columnTypes = ckHelper.getColumnTypes();
    return new ClickHouseRecordWriter(ckHelper, batchSize, tblName, columnNames, columnTypes);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Write operations are not allowed.");
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    // do nothing
  }

}
