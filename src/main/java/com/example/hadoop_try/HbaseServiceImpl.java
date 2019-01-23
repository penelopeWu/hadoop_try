package com.example.hadoop_try;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class HbaseServiceImpl implements HbaseService {
    private static final Logger logger = LoggerFactory.getLogger(HbaseServiceImpl.class);

    @Autowired
    private HbaseConfig hbaseConfig;

    /**
     * 根据tableName，rowkey，family coluem 查询单个数据
     */
    public String getValue(String tableName, String rowKey, String family, String column) {
        Table table = null;
        Connection connection = null;
        String res = "";
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowKey) || StringUtils.isBlank(column)) {
            return null;
        }
        try {
            connection = ConnectionFactory.createConnection(hbaseConfig.configuration());
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addColumn(family.getBytes(), column.getBytes());
            Result result = table.get(get);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    res = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }


    /**
     * 创建表
     *
     * @param tableName         表名
     * @param columnFamily      列族（数组）
     */
    public void createTable(String tableName, String[] columnFamily) throws IOException{
        Connection connection = null;
        TableName name = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        //如果存在则删除
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
            logger.error("create htable error! this table {} already exists!", name);
        } else {
            HTableDescriptor desc = new HTableDescriptor(name);
            for (String cf : columnFamily) {
                desc.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(desc);
        }
    }

}
