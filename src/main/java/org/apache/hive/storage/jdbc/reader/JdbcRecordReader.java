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
package org.apache.hive.storage.jdbc.reader;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.JdbcRecordIterator;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public abstract class JdbcRecordReader implements RecordReader<LongWritable, MapWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordReader.class);
  public DatabaseAccessor dbAccessor = null;
  public JdbcRecordIterator iterator = null;
  public final JobConf conf;
  public int pos = 0;


  public JdbcRecordReader(JobConf conf) {
    LOGGER.trace("Initializing JdbcRecordReader");
    this.conf = conf;
  }

  abstract JdbcRecordIterator getIterator() throws HiveJdbcDatabaseAccessException;


  @Override
  public boolean next(LongWritable key, MapWritable value) throws IOException {
    try {
      LOGGER.trace("JdbcRecordReader.next called");
      iterator = getIterator();
      if (iterator.hasNext()) {
        LOGGER.trace("JdbcRecordReader has more records to read.");
        key.set(pos);
        pos++;
        Map<String, Object> record = iterator.next();
        if ((record != null) && (!record.isEmpty())) {
          for (Entry<String, Object> entry : record.entrySet()) {
            value.put(new Text(entry.getKey()),
                entry.getValue() == null ? NullWritable.get() : new ObjectWritable(entry.getValue()));
          }
          return true;
        }
        else {
          LOGGER.debug("JdbcRecordReader got null record.");
          return false;
        }
      }
      else {
        LOGGER.debug("JdbcRecordReader has no more records to read.");
        return false;
      }
    }catch (Exception e) {
      LOGGER.error("An error occurred while reading the next record from DB.", e);
      return false;
    }
  }


  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }


  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }


  @Override
  public long getPos() throws IOException {
    return pos;
  }


  @Override
  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
    }
  }

  public void setDbAccessor(DatabaseAccessor dbAccessor) {
    this.dbAccessor = dbAccessor;
  }


  public void setIterator(JdbcRecordIterator iterator) {
    this.iterator = iterator;
  }

}
