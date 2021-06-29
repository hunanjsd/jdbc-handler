package org.apache.hive.storage.jdbc.split;

import java.io.Serializable;

/**
 * @className: ColumnSplitConfig
 * @description: TODO 类描述
 * @author: jiangsd3
 * @date: 2021/6/21
 **/
public class ColumnSplitConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String columnName;

    private String columnType;

    private Object columnMinValue;

    private Object columnMaxValue;

    // 默认设置 0 条, split 的可能不相等
    private Long numOfRecords;

    public ColumnSplitConfig(String columnName, String columnType, Object columnMinValue, Object columnMaxValue){
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnMinValue = columnMinValue;
        this.columnMaxValue = columnMaxValue;
        this.numOfRecords = 0L;
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

    public Object getColumnMinValue() {
        return columnMinValue;
    }

    public void setColumnMinValue(Object columnMinValue) {
        this.columnMinValue = columnMinValue;
    }

    public Object getColumnMaxValue() {
        return columnMaxValue;
    }

    public void setColumnMaxValue(Object columnMaxValue) {
        this.columnMaxValue = columnMaxValue;
    }

    public Long getNumOfRecords() {
        return numOfRecords;
    }

    public void setNumOfRecords(Long numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    @Override
    public String toString() {
        return "ColumnSplitConfig{" +
                "columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", columnMinValue=" + columnMinValue +
                ", columnMaxValue=" + columnMaxValue +
                ", numOfRecords=" + numOfRecords +
                '}';
    }
}
