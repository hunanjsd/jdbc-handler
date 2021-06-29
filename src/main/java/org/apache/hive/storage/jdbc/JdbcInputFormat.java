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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hive.storage.jdbc.conf.ConfigConsts;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.reader.GenericLimitJdbcRecordReader;
import org.apache.hive.storage.jdbc.reader.GenericSplitJdbcRecordReader;
import org.apache.hive.storage.jdbc.split.ColumnSplitConfig;
import org.apache.hive.storage.jdbc.split.JdbcInputColumnSplit;
import org.apache.hive.storage.jdbc.split.JdbcInputLimitSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JdbcInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcInputFormat.class);
  private DatabaseAccessor dbAccessor = null;


  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<LongWritable, MapWritable>
    getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    if(split instanceof  JdbcInputLimitSplit){
      return new GenericLimitJdbcRecordReader(job, (JdbcInputLimitSplit) split);
    }else if(split instanceof JdbcInputColumnSplit){
      LOGGER.error("-------------------- split info: " + split);
      return new GenericSplitJdbcRecordReader(job, (JdbcInputColumnSplit) split);
    }else {
      throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {

      LOGGER.debug("Creating {} input splits", numSplits);

      if (dbAccessor == null) {
        dbAccessor = DatabaseAccessorFactory.getAccessor(job);
      }

      String  inputTableSplitColumn = job.get(JdbcStorageConfig.SPLIT_COLUMN.getPropertyName());
      int inputNumberPartition =  job.getInt(JdbcStorageConfig.NUMBER_PARTITION.getPropertyName(), -2);
      if(inputNumberPartition != -2){
        numSplits = inputNumberPartition;
      }
      Path[] tablePaths = FileInputFormat.getInputPaths(job);
      InputSplit[] splits = null;
      // 是否按照 column split map task 任务
      if(StringUtils.isNotEmpty(inputTableSplitColumn)){
        // todo split 默认设置为 10
        numSplits = 10;
        List<ColumnSplitConfig> splitColumnInfoList = dbAccessor.getColumnSplitInfo(job, inputTableSplitColumn, numSplits);
        splits = new InputSplit[splitColumnInfoList.size()];
        for(int i =0; i< splitColumnInfoList.size(); i++){
          ColumnSplitConfig columnSplitConfig = splitColumnInfoList.get(i);
          splits[i] = new JdbcInputColumnSplit(tablePaths[0], columnSplitConfig.getColumnName(), columnSplitConfig.getColumnType(), (Long)columnSplitConfig.getColumnMinValue(), (Long)columnSplitConfig.getColumnMaxValue(), columnSplitConfig.getNumOfRecords().intValue());
        }
      }else {

        int numRecords = numSplits <=1 ? Integer.MAX_VALUE : dbAccessor.getTotalNumberOfRecords(job);

        if (numRecords < numSplits) {
          numSplits = numRecords;
        }

        if (numSplits <= 0) {
          numSplits = 1;
        }

        int numRecordsPerSplit = numRecords / numSplits;
        int numSplitsWithExtraRecords = numRecords % numSplits;

        LOGGER.debug("Num records = {}", numRecords);

        splits = new InputSplit[numSplits];
        int offset = 0;
        for (int i = 0; i < numSplits; i++) {
          int numRecordsInThisSplit = numRecordsPerSplit;
          if (i < numSplitsWithExtraRecords) {
            numRecordsInThisSplit++;
          }

          splits[i] = new JdbcInputLimitSplit(numRecordsInThisSplit, offset, tablePaths[0]);
          offset += numRecordsInThisSplit;
        }
      }

      dbAccessor = null;
      return splits;
    }catch (Exception e) {
      LOGGER.error("Error while splitting input data.", e);
      throw new IOException(e);
    }

  }


  /**
   * For testing purposes only
   *
   * @param dbAccessor
   *            DatabaseAccessor object
   */
  public void setDbAccessor(DatabaseAccessor dbAccessor) {
    this.dbAccessor = dbAccessor;
  }

}
