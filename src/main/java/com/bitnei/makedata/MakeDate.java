package com.bitnei.makedata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lidaxue@bitnei.cn on 2019/5/13.
 */
public class MakeDate {

    private Configuration configuration = HBaseConfiguration.create();
    private Connection connection = null;
    private int vehicleNum = 10000;

    public MakeDate() throws IOException {
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

    /**
     *  frist: 20190420  1 day
     */

    public class LoadDataToHbaseThread implements Runnable{
        private String executeDateStr;
        public LoadDataToHbaseThread(String executeDateStr){
            this.executeDateStr = executeDateStr;
        }
        public void run() {
            HTable htable = null;
            try {
                 htable = (HTable) connection.getTable(TableName.valueOf("realinfo"));
                htable.setAutoFlushTo(false);
                htable.setWriteBufferSize(1024 * 1024 * 30);
                Result result = htable.get(new Get(Bytes.toBytes("01bad09f01af4580a73342916f6e48bb_1556522384000")));
                String rowkeyPrefix = "ldx_test";
                List<Put> list = new ArrayList<Put>(1000);
                List<String> dayStrList = TimeUtil.getDateStr(this.executeDateStr, 1);
                int numIndex = 0;
                for (int i = 4100; i < vehicleNum; i++) {
                    for (String dayStr : dayStrList) {
                        List<Long> timestampList = TimeUtil.getTimestamp(dayStr);
                        for (Long timeStamp : timestampList) {
                            String rowkey = new StringBuilder(Md5Util.encodeByMD5(rowkeyPrefix + i))
                                    .append("_")
                                    .append(timeStamp)
                                    .toString();

                            //System.out.println("the rowkey is :"+rowkey);
                            if(numIndex % 10000 == 0){
                                System.out.println(Thread.currentThread().getName()+","+System.currentTimeMillis()+",the vehicle num is "+i+",the dayStr is:"+dayStr);
                            }
                            list.add(toInsertPut(result, rowkey));
                            if (list.size() > 0 && list.size() % 1000 == 0) {

                                htable.put(list);
                                list.clear();
                                htable.flushCommits();
                            }

                            numIndex ++;

                        }
                    }

                }

                if (list.size() > 0) {
                    htable.put(list);
                }
            }catch(Exception e){
                e.printStackTrace();
            }finally {
                if(htable != null){
                    try {
                        htable.close();
                    }catch(IOException e1){
                        e1.printStackTrace();
                    }
                }
            }
        }
    }
    /*private void makeDateTest(){
        try {
            HTable htable = (HTable) connection.getTable(TableName.valueOf("realinfo"));
            htable.setAutoFlushTo(false);
            htable.setWriteBufferSize(1024 * 1024 * 200);
            Result result = htable.get(new Get(Bytes.toBytes("01bad09f01af4580a73342916f6e48bb_1556522384000")));
            String rowkeyPrefix = "ldx_test";
            List<Put> list = new ArrayList<Put>();
            List<String> dayStrList = TimeUtil.getDateStr("20190420", 1);
            int numIndex = 0;
            for (int i = 0; i < vehicleNum; i++) {
                for (String dayStr : dayStrList) {
                    List<Long> timestampList = TimeUtil.getTimestamp(dayStr);
                    for (Long timeStamp : timestampList) {
                        String rowkey = new StringBuilder(Md5Util.encodeByMD5(rowkeyPrefix + i))
                                .append("_")
                                .append(timeStamp)
                                .toString();

                        //System.out.println("the rowkey is :"+rowkey);
                        if(numIndex % 10000 == 0){
                            System.out.println(System.currentTimeMillis()+",the vehicle num is "+i+",the dayStr is:"+dayStr);
                        }
                        list.add(toInsertPut(result, rowkey));
                        if (list.size() > 0 && list.size() % 10000 == 0) {
                            htable.put(list);
                            list.clear();
                        }

                        numIndex ++;

                    }
                }

            }

            if (list.size() > 0) {
                htable.put(list);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(connection != null){
                try {
                    connection.close();
                }catch(IOException e1){
                    e1.printStackTrace();
                }
            }
        }
    }*/

    private Put toInsertPut(Result result,String rowkey) throws Exception{
        Put put = new Put(Bytes.toBytes(rowkey));
        List<Cell> list = result.listCells();
        for(Cell cell : list){
            put.addColumn(CellUtil.cloneFamily(cell),CellUtil.cloneQualifier(cell),CellUtil.cloneValue(cell));
        }
       return put;
    }

    private void hbaseConnClose(){
         if(connection != null){
             try {
                 connection.close();
             }catch(Exception e){
                 e.printStackTrace();
             }
         }
    }

    public void start() throws  Exception{
        int dayPeriod = 5;
        Thread[] loadThread = new Thread[dayPeriod];
        List<String> dateStrList = TimeUtil.getDateStr("20190420",dayPeriod);
        for(int i = 0 ;i < dayPeriod; i++){
            loadThread[i] = new Thread(new LoadDataToHbaseThread(dateStrList.get(i)),"thread_"+i);
            loadThread[i].start();
        }

        for(int j = 0 ;j< loadThread.length;j++ ){
            loadThread[j].join();
        }
        hbaseConnClose();

        System.out.println("the task finished !");
    }

    public static void main(String[] args) {
        try {
            MakeDate makeDate = new MakeDate();
            makeDate.start();
        }catch(Exception e){

        }
    }
}
