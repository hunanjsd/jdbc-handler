package org.apache.hive.storage.jdbc.writer;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;

import java.io.IOException;
import java.util.Properties;

/**
 * @ClassName: RecordWriteImpl
 * @Author: simo
 * @Date: 2021/6/12 8:28 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public interface RecordWriteImpl {


    /**
     * todo
     * @param tblProps
     * @return
     * @throws Exception
     */
    FileSinkOperator.RecordWriter getRecordWrite(Properties tblProps) throws Exception;
}
