package com.example.hadoop_try;

public interface HbaseService {
    String getValue(String tableName, String rowkey, String family, String column);
}
