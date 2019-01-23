package com.example.hadoop_try;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HadoopTryApplicationTests {

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(HadoopTryApplicationTests.class);

    @Autowired
    private HbaseConfig hbaseConfig;

    @Test
    public void contextLoads() throws IOException {
        Connection connection = ConnectionFactory.createConnection(hbaseConfig.configuration());
        TableName name = TableName.valueOf("test-table");
        String[] columnFamily = new String[]{"a","b"};
        Admin admin = connection.getAdmin();
        if (admin.tableExists(name)) {
            admin.disableTable(name);
            admin.deleteTable(name);
            Logger.error("create htable error this table {} already exists!", name);
        } else {
            HTableDescriptor desc = new HTableDescriptor(name);
            for (String cf:columnFamily){
                desc.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(desc);
        }
    }

}

