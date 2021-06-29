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
package org.apache.hive.storage.jdbc.split;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JdbcInputColumnSplit extends FileSplit implements InputSplit {

  private static final String[] EMPTY_ARRAY = new String[] {};


//  private ColumnSplitConfig columnSplitConfig;
  private String columnName;

  private String columnType;

  private Long columnMinValue;

  private Long columnMaxValue;

  // 默认设置 0 条, split 的可能不相等
  private Long numOfRecords;
  Integer limit = 0;

  public JdbcInputColumnSplit() {
    super((Path) null, 0, 0, EMPTY_ARRAY);
  }

/*  public JdbcInputColumnSplit(ColumnSplitConfig columnSplitConfig, Path dummyPath) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
    this.limit = columnSplitConfig.getNumOfRecords().intValue();
//    this.columnSplitConfig = columnSplitConfig;
  }*/

  public JdbcInputColumnSplit(Path dummyPath, String columnName, String columnType, Long columnMinValue, Long columnMaxValue, Integer limit) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
//    this.limit = columnSplitConfig.getNumOfRecords().intValue();
    this.columnName = columnName;
    this.columnType = columnType;
    this.columnMinValue = columnMinValue;
    this.columnMaxValue = columnMaxValue;
    this.limit = limit;
//    this.columnSplitConfig = columnSplitConfig;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(this.columnName.getBytes().length);
    out.writeBytes(this.columnName);

    out.writeInt(this.columnType.getBytes().length);
    out.writeBytes(this.columnType);

    out.writeLong(this.columnMinValue);
    out.writeLong(this.columnMaxValue);
    out.writeInt(limit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int columnNameLength = in.readInt();
    byte[] columnNameBytes = new byte[columnNameLength];
    in.readFully(columnNameBytes);
    this.columnName = new String(columnNameBytes);

    int columnTypeLength = in.readInt();
    byte[] columnTypeLengthBytes = new byte[columnTypeLength];
    in.readFully(columnTypeLengthBytes);
    this.columnType = new String(columnTypeLengthBytes);

    this.columnMinValue = in.readLong();
    this.columnMaxValue = in.readLong();

    this.limit = in.readInt();
  }


  @Override
  public long getLength() {
    return this.limit;
  }


  @Override
  public String[] getLocations() throws IOException {
    return EMPTY_ARRAY;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnType() {
    return columnType;
  }

  public void setColumnType(String columnType) {
    this.columnType = columnType;
  }

  public Long getColumnMinValue() {
    return columnMinValue;
  }

  public void setColumnMinValue(Long columnMinValue) {
    this.columnMinValue = columnMinValue;
  }

  public Long getColumnMaxValue() {
    return columnMaxValue;
  }

  public void setColumnMaxValue(Long columnMaxValue) {
    this.columnMaxValue = columnMaxValue;
  }

  public Long getNumOfRecords() {
    return numOfRecords;
  }

  public void setNumOfRecords(Long numOfRecords) {
    this.numOfRecords = numOfRecords;
  }

  //  public ColumnSplitConfig getColumnSplitConfig() {
//    return columnSplitConfig;
//  }


  @Override
  public String toString() {
    return "JdbcInputColumnSplit{" +
            "columnName='" + columnName + '\'' +
            ", columnType='" + columnType + '\'' +
            ", columnMinValue=" + columnMinValue +
            ", columnMaxValue=" + columnMaxValue +
            ", numOfRecords=" + numOfRecords +
            ", limit=" + limit +
            '}';
  }
}
