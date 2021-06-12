package org.apache.hive.storage.jdbc.clickhouse;

import java.util.List;

/**
 * @className: StringUtil
 * @description: TODO 类描述
 * @author: jiangsd3
 * @date: 2021/6/10
 **/
public class StringUtil {

    public static String join(Object[] inputArr, String separator){
        StringBuilder csvBuilder = new StringBuilder();
        for(Object value : inputArr){
            csvBuilder.append(value);
            csvBuilder.append(separator);
        }
        return csvBuilder.toString();
    }
}
