package org.apache.hive.storage.jdbc.writer;

import org.apache.hive.storage.jdbc.conf.DatabaseType;

/**
 * @ClassName: RecordWriteFactory
 * @Author: simo
 * @Date: 2021/6/12 8:27 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public class RecordWriteFactory {

    public static RecordWriteImpl getRecordWrite(DatabaseType dbType) {
        RecordWriteImpl recordWrite;
        switch (dbType) {
            case CLICKHOUSE:
                recordWrite =  new ClickhouseRecordWrite();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Database source : %s  writes are not allowed", dbType.name()));
        }

        return recordWrite;
    }
}
