package org.apache.hive.storage.jdbc.serde;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.dao.AbstractDatabaseAccessor;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Preconditions;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author simo
 */
public class ClickHouseRecordWriter implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseRecordWriter.class);
    private final int batchSize;
    private final List<String> clickhouseColNames;
    private final List<String> clickhouseColTypes;
    private final String insertQuery;
    private final AbstractDatabaseAccessor databaseAccessor;

    private final ArrayList<Map<String, Object>> data = new ArrayList<>();

    private long recordCoutner = 0L;

    public ClickHouseRecordWriter(AbstractDatabaseAccessor databaseAccessor, int batchSize, String tableName) {
        this.batchSize = batchSize;
        this.clickhouseColNames = databaseAccessor.getColumnNames();
        this.clickhouseColTypes = databaseAccessor.getColumnTypes();
        this.insertQuery = constructInsertQuery(tableName, databaseAccessor.getColumnNames());
        this.databaseAccessor = databaseAccessor;
    }

    public static String constructInsertQuery(String tableName, List<String> columnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" ");

        String fields = StringUtils.join(columnNames.toArray(), ",");
        String[] valueSlice = new String[columnNames.size()];
        Arrays.fill(valueSlice, "?");

        String values = StringUtils.join(valueSlice, ",");
        sql.append("(").append(fields).append(") VALUES (").append(values).append(")");
        return sql.toString();
    }

    private static void addValuesToBatch(Map<String, Object> value, PreparedStatement stmt, List<String> columnNames,
            List<String> columnTypes) throws SQLException {
        Preconditions.checkArgument(columnNames.size() == columnTypes.size(),
                "Column types and column names must be matched");
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String columnType = columnTypes.get(i);
            Object obj = value.containsKey(columnName) ? value.get(columnName) : null;
            if (obj != null) {
                switch (columnType) {
                    case "Int8":
                    case "UInt8":
                        stmt.setByte(i + 1, (byte) obj);
                        break;
                    case "Int16":
                    case "UInt16":
                        stmt.setShort(i + 1, (short) obj);
                        break;
                    case "Int32":
                    case "UInt32":
                        stmt.setInt(i + 1, (int) obj);
                        break;
                    case "Int64":
                    case "UInt64":
                        stmt.setLong(i + 1, (long) obj);
                        break;
                    case "Float32":
                        stmt.setFloat(i + 1, (float) obj);
                        break;
                    case "Float64":
                        stmt.setDouble(i + 1, (double) obj);
                        break;
                    case "String":
                        stmt.setString(i + 1, (String) obj);
                        break;
                    case "DateTime":
                        stmt.setTimestamp(i + 1, (Timestamp) obj);
                        break;
                    case "Date":
                        stmt.setDate(i + 1, (Date) obj);
                        break;
                    default:
                        if(columnType.toUpperCase().contains("DECIMAL")){
                            stmt.setBigDecimal(i + 1, (BigDecimal)obj);
                        }else {
                            throw new SQLException(String.format("Un-supported type %s", columnType));
                        }
                }
            } else {
                switch (columnType) {
                case "Int8":
                case "UInt8":
                    stmt.setByte(i + 1, (byte) 0);
                    break;
                case "Int16":
                case "UInt16":
                    stmt.setShort(i + 1, (byte) 0);
                    break;
                case "Int32":
                case "UInt32":
                    stmt.setInt(i + 1, 0);
                    break;
                case "Int64":
                case "UInt64":
                    stmt.setLong(i + 1, 0L);
                    break;
                case "Float32":
                    stmt.setFloat(i + 1, 0.0f);
                    break;
                case "Float64":
                    stmt.setDouble(i + 1, 0.0);
                    break;
                case "String":
                    stmt.setString(i + 1, "");
                    break;
                case "DateTime":
                    stmt.setTimestamp(i + 1, new Timestamp(DateTime.now().getMillis()));
                    break;
                case "Date":
                    stmt.setDate(i + 1, new Date(DateTime.now().getMillis()));
                    break;
                default:
                    if(columnType.toUpperCase().contains("DECIMAL")){
                        stmt.setBigDecimal(i + 1, new BigDecimal(0));
                    }else {
                        throw new SQLException(String.format("Un-supported type %s", columnType));
                    }
                }
            }

        }
        stmt.addBatch();
    }

    // reall does the flush
    private void doFlush() throws IOException {
        if (data.isEmpty()) {
            return;
        }
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection =  databaseAccessor.getDbcpDataSource().getConnection();
            statement = connection.prepareStatement(this.insertQuery);

            for (Map<String, Object> value : data) {
                addValuesToBatch(value, statement, clickhouseColNames, clickhouseColTypes);
            }
            statement.executeBatch();

            recordCoutner += data.size();
            LOGGER.info(String.format("Flushed %s rows of data, total flushed %s row of data.", data.size(), recordCoutner));
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error("Error closing resource", e);
            }
        }
    }

    public void flush(int retry) throws IOException {
        try {
            while (retry-- > 0) {
                try {
                    doFlush();
                    break;
                } catch (Exception e) {
                    LOGGER.error("Error flushing, retrying", e);
                }
            }
        } finally {
            data.clear();
        }
    }

    // Write the data, the writable comes from the ClickHouseSerDe
    @Override
    public void write(Writable w) throws IOException {
        ClickHouseWritable ckWritable = (ClickHouseWritable) w;
        data.add(ckWritable.getValue());
        if (data.size() >= batchSize) {
            flush(3);
        }
    }

    @Override
    public void close(boolean abort) throws IOException {
        LOGGER.info("Closing Writer, flush remaining: " + data.size());
        flush(3);
    }
}
