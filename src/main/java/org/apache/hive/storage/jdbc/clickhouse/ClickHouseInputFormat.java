package org.apache.hive.storage.jdbc.clickhouse;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * A dummpy implementation of input format, read is not supported
 */
public class ClickHouseInputFormat implements InputFormat {
    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return null;
    }
}
