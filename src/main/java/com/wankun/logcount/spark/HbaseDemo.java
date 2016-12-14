package com.wankun.logcount.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * HBase使用例子
 */
public class HbaseDemo {
    private final static Logger logger = LoggerFactory.getLogger(HbaseDemo.class);
    static Configuration hbaseConfiguration = HBaseConfiguration.create();

    static {
        hbaseConfiguration.addResource("hbase-site.xml");
    }

    /**
     * 创建表
     *
     * @param tablename    表名
     * @param columnFamily 列族
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static void CreateTable(String tablename, String columnFamily)
            throws MasterNotRunningException, ZooKeeperConnectionException,
            IOException {
        System.out.println(hbaseConfiguration.get("hbase.zookeeper.quorum"));
        HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
        if (admin.tableExists(tablename)) {// 如果表已经存在
            System.out.println(tablename + "表已经存在!----------");
        } else {
            TableName tableName = TableName.valueOf(tablename);
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println(tablename + "表已经成功创建!----------------");
        }
    }

    /**
     * 向表中插入一条新数据
     *
     * @param tableName    表名
     * @param row          行键key
     * @param columnFamily 列族
     * @param column       列名
     * @param data         要插入的数据
     * @throws IOException
     */
    public static void PutData(String tableName, String row,
                               String columnFamily, String column, String data) throws IOException {
        HTable table = new HTable(hbaseConfiguration, tableName);
        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));
        table.put(put);
        System.out.println("-------put '" + row + "','" + columnFamily + ":" + column
                + "','" + data + "'");
    }

    /**
     * 获取指定行的所有数据
     *
     * @param tableName    表名
     * @param row          行键key
     * @param columnFamily 列族
     * @param column       列名
     * @throws IOException
     */
    public static void GetData(String tableName, String row,
                               String columnFamily, String column) throws IOException {
        HTable table = new HTable(hbaseConfiguration, tableName);
        // Scan scan = new Scan();
        // ResultScanner result = table.getScanner(scan);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        byte[] rb = result.getValue(Bytes.toBytes(columnFamily),
                Bytes.toBytes(column));
        String value = new String(rb, "UTF-8");
        System.out.println("------" + value);
    }

    /**
     * 获取指定表的所有数据
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void ScanAll(String tableName) throws IOException {
        HTable table = new HTable(hbaseConfiguration, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] rb = cell.getValueArray();
                String row = new String(result.getRow(), "UTF-8");
                String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
                String qualifier = new String(CellUtil.cloneQualifier(cell),
                        "UTF-8");
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                System.out.println(":::::[row:" + row + "],[family:" + family
                        + "],[qualifier:" + qualifier + "],[value:" + value
                        + "]");
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            HbaseDemo.CreateTable("userinfo", "baseinfo");
            HbaseDemo.PutData("userinfo", "row2", "baseinfo", "vio2",
                    "驾驶车辆违法信息2：");
            HbaseDemo.PutData("userinfo", "row5", "baseinfo", "vio2",
                    "驾驶车辆违法信息2：");
            HbaseDemo.GetData("userinfo", "row2", "baseinfo", "vio2");
            HbaseDemo.ScanAll("userinfo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}