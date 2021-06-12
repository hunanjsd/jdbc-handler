package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.conf.Configuration;

/**
 * @className: ClickhouseDatabaseAccessor
 * @description: Clickhouse specific data accessor
 * @author: jiangsd3
 * @date: 2021/5/14
 **/
public class ClickhouseDatabaseAccessor extends GenericJdbcDatabaseAccessor {


    public ClickhouseDatabaseAccessor(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
        if (offset == 0) {
            return addLimitToQuery(sql, limit);
        }
        else {
            return sql + " LIMIT " + offset + "," + limit;
        }
    }


    @Override
    protected String addLimitToQuery(String sql, int limit) {
        return sql + " LIMIT " + limit ;
    }

}
