package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 *
 * @author zhouhui
 */
public class HbaseTest {
    private static final Logger logger = LoggerFactory.getLogger(HbaseTest.class);

    HBaseAdmin hBaseAdmin;
    HTable table;
    String tableName = "phone";

    @Before
    public void init() throws IOException {
        Configuration configuration = new Configuration();
        //连接hbase，指定hbase使用的集群就OK了
        configuration.set("hbase.zookeeper.quorum", "node2,node3,node4");
        hBaseAdmin = new HBaseAdmin(configuration);
        table = new HTable(configuration, tableName);
    }

    @After
    public void destroy() throws IOException {
        if(hBaseAdmin != null){
            hBaseAdmin.close();
        }
        if(table != null){
            table.close();
        }
    }


    @Test
    public void create() throws IOException {
        if(hBaseAdmin.tableExists(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor cf = new HColumnDescriptor("cf");
        cf.setInMemory(true);
        cf.setMaxVersions(10);
        ///cf.setTimeToLive(1);
        desc.addFamily(cf);

        hBaseAdmin.createTable(desc);
        logger.debug("logger");
    }

    @Test
    public void insert() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put("123".getBytes());

        put.add("cf".getBytes(), "sex".getBytes(), "female".getBytes());
        put.add("cf".getBytes(), "name".getBytes(), "zhouhui".getBytes());

        table.put(put);
    }

    @Test
    public void get() throws IOException {
        Get get = new Get("123".getBytes());
        get.addColumn("cf".getBytes(), "name".getBytes());
        Result result = table.get(get);

        Cell cell = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        String name = new String(CellUtil.cloneValue(cell));
        System.out.println(name);
    }
}
