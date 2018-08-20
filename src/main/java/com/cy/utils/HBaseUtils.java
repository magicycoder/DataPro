package com.cy.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public class HBaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "localhost:2181");
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /***
     * 根据表名获取HTable实例
     * @param tablename
     * @return
     */
    public HTable getTable(String tablename) {
        HTable tbl = null;
        try {
            tbl = new HTable(configuration, tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tbl;
    }

    public static void main(String[] args) {

        HTable table = HBaseUtils.getInstance().getTable("course_click");

        System.out.println(table.getName().getNameAsString());
    }
}
