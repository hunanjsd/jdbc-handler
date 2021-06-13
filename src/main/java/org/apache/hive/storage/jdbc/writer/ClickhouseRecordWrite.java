package org.apache.hive.storage.jdbc.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.AbstractDatabaseAccessor;
import org.apache.hive.storage.jdbc.serde.ClickHouseRecordWriter;
import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @ClassName: ClickhouseRecordWrite
 * @Author: simo
 * @Date: 2021/6/12 8:30 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public class ClickhouseRecordWrite implements RecordWriteImpl{
    private final static Logger LOGGER = LoggerFactory.getLogger(ClickhouseRecordWrite.class);
    public static final int DEFAULT_BATCH_SIZE = 500;
    @Override
    public FileSinkOperator.RecordWriter getRecordWrite(Properties tblProps) throws Exception {
        Set<Map.Entry<Object, Object>> entries = tblProps.entrySet();
        for (Map.Entry<Object, Object> entry: entries) {
            LOGGER.info(entry.getKey() + " : " + entry.getValue());
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
                batchSize = DEFAULT_BATCH_SIZE;
            } else {
                batchSize = Integer.parseInt(batchSizeStr);
            }
        } catch (NumberFormatException e) {
            LOGGER.info(String.format("Parsing %s failed, use default", batchSizeStr), e);
            batchSize = DEFAULT_BATCH_SIZE;
        }

        Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(tblProps);
        AbstractDatabaseAccessor databaseAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);

        return new ClickHouseRecordWriter(databaseAccessor, batchSize, tblName);
    }
}
