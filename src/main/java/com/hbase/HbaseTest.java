package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author zhouhui
 */
public class HbaseTest {
    HBaseAdmin hadmin;
    HTable hTable;
    String TN = "phone";

    @Before
    public void begin() throws Exception {
        Configuration conf = new Configuration();
        // 连接hbase  指定hbase使用的ZooKeeper集群
        // 伪分布式：？
        conf.set("hbase.zookeeper.quorum", "node2,node3,node4");

        hadmin = new HBaseAdmin(conf);
        hTable = new HTable(conf, TN);
    }

    @After
    public void end() throws Exception {
        if(hadmin != null) {
            hadmin.close();
        }
        if(hTable != null) {
            hTable.close();
        }
    }

    @Test
    public void createTable() throws Exception {
        if(hadmin.tableExists(TN)) {
            hadmin.disableTable(TN);
            hadmin.deleteTable(TN);
        }
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));

        HColumnDescriptor cf = new HColumnDescriptor("cf".getBytes());
        cf.setInMemory(true);
        cf.setMaxVersions(1);
        desc.addFamily(cf);

        hadmin.createTable(desc);
    }

    @Test
    public void insertDB1() throws Exception {
        Put put = new Put("111".getBytes());
        put.add("cf".getBytes(), "name".getBytes(), "xiaoming".getBytes());
        put.add("cf".getBytes(), "sex".getBytes(), "man".getBytes());
        hTable.put(put);
    }

    @Test
    public void getDB1() throws Exception {
        Get get = new Get("111".getBytes());
        get.addColumn("cf".getBytes(), "name".getBytes());

        Result rs = hTable.get(get);
        Cell cell = rs.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        System.out.println(new String(CellUtil.cloneValue(cell)));
    }

    /**
     * 十个用户（手机号）  每个用户随机产生一百条通话记录
     * rowkey设计：手机号_(max-时间戳)
     */
    @Test
    public void insertDB2() throws Exception {
        List<Put> puts = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            // 自己手机号
            String phoneNum = getPhoneNum("186");

            for (int j = 0; j < 100; j++) {
                // 对方手机号
                String dNum = getPhoneNum("177");
                // 通话时间
                String dateStr = getDate("2017");
                // 主叫被叫类型 0  1
                String type = r.nextInt(2) + "";

                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

                String rowkey = phoneNum + "_" + (Long.MAX_VALUE-sdf.parse(dateStr).getTime());

                Put put = new Put(rowkey.getBytes());
                put.add("cf".getBytes(), "dnum".getBytes(), dNum.getBytes());
                put.add("cf".getBytes(), "date".getBytes(), dateStr.getBytes());
                put.add("cf".getBytes(), "type".getBytes(), type.getBytes());

                puts.add(put);
            }
        }

        hTable.put(puts);
    }

    /**
     * 查询某个用户（手机号） 某个月份所有的通话记录
     * 通话记录  降序排序
     */
    @Test
    public void scanDB1() throws Exception {
        // 查询 18696773848  2月份 所有的通话记录
        Scan scan = new Scan();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        String startRowKey = "18686891512_" + (Long.MAX_VALUE - sdf.parse("20170301000000").getTime());
        String stopRowKey = "18686891512 _" + (Long.MAX_VALUE - sdf.parse("20170201000000").getTime());

        scan.setStartRow(startRowKey.getBytes());
        scan.setStopRow(stopRowKey.getBytes());

        ResultScanner rss = hTable.getScanner(scan);
        for(Result rs : rss) {
            System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print(" " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
            System.out.println(" " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
        }
    }

    /**
     * 查询某个手机号   所有的主叫类型（type=1）的通话记录
     * 过滤器
     */
    @Test
    public void scanDB2() throws Exception {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 前缀过滤
        PrefixFilter filter1 = new PrefixFilter("18696773848".getBytes());
        list.addFilter(filter1);

        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(), CompareFilter.CompareOp.EQUAL, "1".getBytes());
        list.addFilter(filter2);

        Scan scan = new Scan();
        scan.setFilter(list);

        ResultScanner rss = hTable.getScanner(scan);
        for(Result rs : rss) {
            System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print(" " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
            System.out.println(" " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
        }
    }

    // 删除cell

    Random r = new Random();

    /**
     * 随机生成测试手机号码 prefix: 手机号码前缀 eq:186
     */
    public String getPhoneNum(String prefix) {
        return prefix + String.format("%08d", r.nextInt(99999999));
    }

    /**
     * 随机生成时间
     *
     * @param year 年
     * @return 时间 格式：yyyyMMddHHmmss
     */
    public String getDate(String year) {
        return year
                + String.format(
                "%02d%02d%02d%02d%02d",
                new Object[] { r.nextInt(12) + 1, r.nextInt(28) + 1,
                        r.nextInt(24), r.nextInt(60), r.nextInt(60) });
    }
}
