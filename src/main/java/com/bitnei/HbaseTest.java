package com.bitnei;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by lidaxue@bitnei.cn on 2019/5/10.
 */
public class HbaseTest {
    private Configuration configuration = HBaseConfiguration.create();
    private Connection connection = null;

    public HbaseTest() throws IOException{
        initConfiguration();
        connection = ConnectionFactory.createConnection(configuration);
    }
    private void initConfiguration(){
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.zookeeper.quorum","cnqycspser01,cnqycspser02,cnqycspser03");
        configuration.set("hbase.master","cnqycspser01:60000");
        configuration.setLong("hbase.client.scanner.timeout.period",5 * 60 * 1000L);
        configuration.setInt("hbase.rpc.timeout",5 * 60 * 1000);
    }

    private void scanTest(){
        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf("realinfo"));
            hTable.setAutoFlushTo(false);
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(10);
            long startTime = System.currentTimeMillis();
            //93a1ee68bf47416a84b4abd2af2b6473_1557556671000
            // fuzzy scan 20190511-> (155750,155759)  HbaseRowkeyFilter.getFuzzyRowkeyFilter("155750","155759")
            scan.setFilter(HbaseRowkeyFilter.getFuzzyRowkeyFilter("15575","15576"));
            ResultScanner scanner = hTable.getScanner(scan);
            for(Result result : scanner){
               String rowkey =  Bytes.toString(result.getRow());
               System.out.println("rowkey:"+rowkey);
            }
            System.out.println("the cost time is :"+(System.currentTimeMillis()-startTime));
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(connection != null) {
                try {
                    connection.close();
                }catch(Exception e1){
                    e1.printStackTrace();
                }
            }
        }
    }

    private Filter getRowkeyFilter(){
        String regexKey = "[\\dA-Fa-f]{32}_((15575|15576)[\\d]{8}){1}";
        RegexStringComparator regexStringComparator = new RegexStringComparator(regexKey);
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,regexStringComparator);
        return rowFilter;
    }



    public static void main(String[] args) {
        try {
            HbaseTest hbaseTest = new HbaseTest();
            hbaseTest.scanTest();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
