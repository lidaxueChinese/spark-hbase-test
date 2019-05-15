package com.bitnei.phoenix;

import com.bitnei.HbaseRowkeyFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.List;

/**
 * Created by lidaxue@bitnei.cn on 2019/5/14.
 */
public class PhoenixText {

    private Configuration configuration = HBaseConfiguration.create();
    private Connection connection = null;

    public PhoenixText() throws IOException {
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
            HTable hTable = (HTable) connection.getTable(TableName.valueOf("LDX_PHEONIX_TEST"));
            hTable.setAutoFlushTo(false);
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(10);
            int index = 1;
            while(index<10){
                Put put = new Put(Bytes.toBytes("rowkey0"+index));
                put.addColumn(Bytes.toBytes("CF"),Bytes.toBytes("NAME"),Bytes.toBytes("lidaxue0"+index));
                put.addColumn(Bytes.toBytes("CF"),Bytes.toBytes("AGE"),Bytes.toBytes(12+index));
                hTable.put(put);
                hTable.flushCommits();
                System.out.println("the flush...");
                Threads.sleep(1000);
                index ++;
            }
            /*Get get = new Get(Bytes.toBytes("rowkey1"));
            Result result = hTable.get(get);
            List<Cell> listCell = result.listCells();
            for(Cell cell : listCell){
               String qualifer =  Bytes.toString(CellUtil.cloneQualifier(cell));
               System.out.println("the qualifer is :"+qualifer);
            }
            System.out.println(result);*/
            /*ResultScanner scanner = hTable.getScanner(scan);
            for(Result result : scanner){
                String rowkey =  Bytes.toString(result.getRow());
                System.out.println("rowkey:"+rowkey);
            }*/
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

    public static void main(String[] args)  throws Exception{
        PhoenixText phoenixText = new PhoenixText();
        phoenixText.scanTest();

    }

}
