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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.dao.JdbcRecordIterator;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.split.JdbcInputLimitSplit;

import java.io.IOException;

public class GenericLimitJdbcRecordReader extends JdbcRecordReader {

  private final JdbcInputLimitSplit split;

  public GenericLimitJdbcRecordReader(JobConf conf, JdbcInputLimitSplit split) {
    super(conf);
    this.split = split;
  }

  public JdbcRecordIterator getIterator() throws HiveJdbcDatabaseAccessException {
    if (dbAccessor == null) {
      dbAccessor = DatabaseAccessorFactory.getAccessor(this.conf);
    }
    return dbAccessor.getRecordIterator(conf, split.getLimit(), split.getOffset());
  }

  @Override
  public float getProgress() throws IOException {
    if (split == null) {
      return 0;
    }
    else {
      return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
    }
  }


}
