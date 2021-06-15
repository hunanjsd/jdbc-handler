package org.apache.hive.storage.jdbc.dao;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * @ClassName: AbstractDatabaseAccessor
 * @Author: simo
 * @Date: 2021/6/12 11:22 PM
 * @Version: 1.0
 * @Description: TODO
 **/

public abstract class AbstractDatabaseAccessor implements DatabaseAccessor{
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractDatabaseAccessor.class);

    protected static final String DBCP_CONFIG_PREFIX = JdbcStorageConfigManager.CONFIG_PREFIX + ".dbcp";
    protected static final Text DBCP_PWD = new Text(DBCP_CONFIG_PREFIX + ".password");
    protected List<String> columnNames = new ArrayList<>();
    protected List<String> columnTypes = new ArrayList<>();
    protected HashMap<String, String> nameTypeMap = new HashMap<>();
    protected DataSource dbcpDataSource = null;
    protected Configuration configuration;

    public AbstractDatabaseAccessor(Configuration configuration){
        this.configuration = configuration;
        initializeDatabaseConnection();
        try {
            initColumnNamesAndTypesFromSystemQuery(configuration);
        } catch (HiveJdbcDatabaseAccessException e) {
            LOGGER.error("init database connection failed ", e);
        }
    }


    protected void initializeDatabaseConnection() {
        try {
            if (this.dbcpDataSource == null) {
                synchronized (this) {
                    if (this.dbcpDataSource == null) {
                        Properties props = getConnectionPoolProperties(this.configuration);
                        this.dbcpDataSource = BasicDataSourceFactory.createDataSource(props);
                    }
                }
            }
        } catch (HiveJdbcDatabaseAccessException e) {
            LOGGER.error("init database connection failed ", e);
        }catch (Exception ee){
            LOGGER.error("init database connection failed ", ee);
        }
    }

    public DataSource getDbcpDataSource(){
        if(this.dbcpDataSource == null){
            initializeDatabaseConnection();
        }
        return this.dbcpDataSource;
    }

    public void initColumnNamesAndTypesFromSystemQuery(Configuration conf) throws HiveJdbcDatabaseAccessException {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            initializeDatabaseConnection();
            String metadataQuery = getMetaDataQuery(conf);
            LOGGER.debug("Query to execute is [{}]", metadataQuery);

            conn = dbcpDataSource.getConnection();
            ps = conn.prepareStatement(metadataQuery);
            rs = ps.executeQuery();

            ResultSetMetaData metadata = rs.getMetaData();
            int numColumns = metadata.getColumnCount();
            for (int i = 0; i < numColumns; i++) {
                metadata.getColumnType(i + 1);
                columnNames.add(metadata.getColumnName(i + 1));
                columnTypes.add(metadata.getColumnTypeName(i + 1));
                nameTypeMap.put(metadata.getColumnName(i + 1), metadata.getColumnTypeName(i + 1));
            }
        }catch (Exception e) {
            LOGGER.error("Error while trying to get column names.", e);
            throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
        }
        finally {
            cleanupResources(conn, ps, rs);
        }

    }

    protected Properties getConnectionPoolProperties(Configuration conf) throws IOException {
        // Create the default properties object
        Properties dbProperties = getDefaultDBCPProperties();

        // override with user defined properties
        Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
        if ((userProperties != null) && (!userProperties.isEmpty())) {
            for (Map.Entry<String, String> entry : userProperties.entrySet()) {
                dbProperties.put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
            }
        }

        // handle password
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        if (credentials.getSecretKey(DBCP_PWD) != null) {
            LOGGER.info("found token in credentials");
            dbProperties.put(DBCP_PWD,new String(credentials.getSecretKey(DBCP_PWD)));
        }

        // essential properties that shouldn't be overridden by users
        dbProperties.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
        dbProperties.put("driverClassName", conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()));
        dbProperties.put("type", "javax.sql.DataSource");
        return dbProperties;
    }


    protected Properties getDefaultDBCPProperties() {
        Properties props = new Properties();
        props.put("initialSize", "1");
        props.put("maxActive", "3");
        props.put("maxIdle", "0");
        props.put("maxWait", "10000");
        props.put("timeBetweenEvictionRunsMillis", "30000");
        return props;
    }

    protected String getMetaDataQuery(Configuration conf) {
        String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
        return addLimitToQuery(sql, 1);
    }

    protected String addLimitToQuery(String sql, int limit) {
        return sql + " {LIMIT " + limit + "}";
    }

    protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Caught exception during resultset cleanup.", e);
        }

        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Caught exception during statement cleanup.", e);
        }

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Caught exception during connection cleanup.", e);
        }
    }

}
