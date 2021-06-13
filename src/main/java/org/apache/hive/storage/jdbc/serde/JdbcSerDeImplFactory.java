package org.apache.hive.storage.jdbc.serde;

import org.apache.hive.storage.jdbc.conf.DatabaseType;

/**
 * @ClassName: JdbcSerDeFactory
 * @Author: simo
 * @Date: 2021/6/12 7:56 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public class JdbcSerDeImplFactory {

    public static JdbcSerDeImpl getJdbcSerDeImpl(DatabaseType dbType) throws UnsupportedOperationException {

        JdbcSerDeImpl jdbcSerDe;
        switch (dbType) {
            // 目前只支持 Clickhouse 写入
            case CLICKHOUSE:
                jdbcSerDe =  new ClickhouseJdbcSerDeImpl();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Database source : %s  writes are not allowed", dbType.name()));
        }
        return jdbcSerDe;
    }
}
