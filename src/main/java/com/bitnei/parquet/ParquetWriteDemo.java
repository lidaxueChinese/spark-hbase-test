package com.bitnei.parquet;

import com.bitnei.makedata.Md5Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import scala.tools.reflect.FormatInterpolator;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lidaxue@bitnei.cn on 2019/5/20.
 */
public class ParquetWriteDemo {

    private static  Connection connection = null;

    private static Configuration parquetConf = null;

    private static MessageType schema = null;

    private  static ThreadPoolExecutor pool = null;

    private static AtomicBoolean isException = new AtomicBoolean(false);

    private static final String HDFS_PATH_PREFIX = "/tmp/spark/vehicle/result/term_result/parquetfile/";

    private static  int batch = 100; //每个线程处理的车辆数

    private static void initHbaseConn() throws IOException{
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.zookeeper.quorum","cnqycspser01,cnqycspser02,cnqycspser03");
        configuration.set("hbase.master","cnqycspser01:60000");
        configuration.setLong("hbase.client.scanner.timeout.period",5 * 60 * 1000L);
        configuration.setInt("hbase.rpc.timeout",5 * 60 * 1000);
        connection = ConnectionFactory.createConnection(configuration);
    }

    private static void initParquet(){
        parquetConf = new Configuration();
        parquetConf.set("fs.defaultFS","hdfs://nameservice1");
        parquetConf.set("dfs.nameservices","nameservice1");
        parquetConf.set("dfs.ha.namenodes.nameservice1","namenode25,namenode59");
        parquetConf.set("dfs.namenode.rpc-address.nameservice1.namenode25","cnqycspser01:8020");
        parquetConf.set("dfs.namenode.rpc-address.nameservice1.namenode59","cnqycspser02:8020");
        parquetConf.set("dfs.client.failover.proxy.provider.nameservice1","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        parquetConf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");

        schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary VID (UTF8);\n" +
                " required binary str7615 (UTF8);\n" +
                "}");
    }

    public ParquetWriteDemo() throws IOException{
       initHbaseConn();
       initParquet();
       pool = new ThreadPoolExecutor(1,120, 5, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("parquetWriteDemo"));
    }

    public static void parquetWrite() throws Exception{

        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(HDFS_PATH_PREFIX+"javaToParquetTest.parquet");
        /*Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://nameservice1");
        configuration.set("dfs.nameservices","nameservice1");
        configuration.set("dfs.ha.namenodes.nameservice1","namenode25,namenode59");
        configuration.set("dfs.namenode.rpc-address.nameservice1.namenode25","cnqycspser01:8020");
        configuration.set("dfs.namenode.rpc-address.nameservice1.namenode59","cnqycspser02:8020");
        configuration.set("dfs.client.failover.proxy.provider.nameservice1","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        configuration.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");*/
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema,parquetConf);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path,parquetConf,writeSupport);
        for(int i = 0 ;i<10;i++){
            Group group = factory.newGroup().append("VID","vid"+i).append("str7615","7615_"+i);
            writer.write(group);
        }

        System.out.println("write end");
        writer.close();
    }
    private class WriteToParquetThread implements Runnable{

        private  int numIndex;
        private boolean isShutdown = false;
        public WriteToParquetThread(int numIndex,boolean isShutdown){
            this.numIndex = numIndex;
            this.isShutdown = isShutdown;
        }
        @Override
        public void run() {
            flagShutDown();

            HTable htable = null;
            ParquetWriter<Group> writer = null;
            try {
                htable = (HTable) connection.getTable(TableName.valueOf("realinfo"));
                //scan
                Scan scan = new Scan();
                scan.setCaching(100);
                scan.setCacheBlocks(false);
                //parquet
                GroupFactory factory = new SimpleGroupFactory(schema);
                Path path = new Path(HDFS_PATH_PREFIX + "javaToParquetTest"+numIndex+".parquet");
                GroupWriteSupport writeSupport = new GroupWriteSupport();
                writeSupport.setSchema(schema, parquetConf);
                writer = new ParquetWriter<Group>(path, parquetConf, writeSupport);

                String[] timestampPrefixArr  = {"15559","15560"};
                for(int i = numIndex;i<numIndex+batch;i++){
                    StringBuilder vinPrefixBuider = new StringBuilder(Md5Util.encodeByMD5("ldx_test"+i)).append("_");
                  for(String timestampPrefix : timestampPrefixArr) {

                      scan.setRowPrefixFilter(Bytes.toBytes(vinPrefixBuider.append(timestampPrefix).toString()));
                      ResultScanner resultScanner = htable.getScanner(scan);
                      for (Result result : resultScanner) {
                          String rowkey = Bytes.toString(result.getRow());
                          long timestamp = 0L;
                          try {
                              timestamp = new Long(rowkey.split("_")[1]);
                          } catch (Exception e) {
                              e.printStackTrace();
                              System.out.println("the exception rowkey is :" + rowkey);
                          }
                          if(isValidDate(timestamp, 1555948800000L, 1556035200000L)){
                              NavigableMap<byte[], byte[]> cfMap = result.getFamilyMap(Bytes.toBytes("cf"));
                              String VID = "-1";
                              if (cfMap.containsKey(Bytes.toBytes("VID"))) {
                                  VID = Bytes.toString(cfMap.get(Bytes.toBytes("VID")));
                              }
                              String str7615 = "-1";
                              if (cfMap.containsKey(Bytes.toBytes("7615"))) {
                                  str7615 = Bytes.toString(cfMap.get(Bytes.toBytes("7615")));
                              }
                              Group group = factory.newGroup().append("VID", VID).append("str7615", str7615);

                              writer.write(group);
                          }

                      }
                  }
                }

            }catch(Exception e){
                e.printStackTrace();

                isException.set(true);
            }finally {
                try {

                    if(writer != null){
                        writer.close();
                    }

                    if (htable != null) {
                        htable.close();
                    }
                }catch(Exception e1){
                    e1.printStackTrace();
                }

            }
        }

        private boolean isValidDate(long timestamp,long startTimestamp,long endTimestamp){
             if(timestamp >= startTimestamp && timestamp < endTimestamp){
                 return true;
             }else{
                 return false;
             }
        }

        private void flagShutDown(){
            //最后一批任务提交后
            if(this.isShutdown) {
                System.out.println("the last task is executed ,the pool is set shutdown ");
                pool.shutdown();
            }
        }
    }

    private void start(){
         int vehicleTotalNum = 10000;
         int index = 0 ;
         while (index < vehicleTotalNum){
             while(pool.getActiveCount() >=100){
                 try {
                     System.out.println("the index is :"+index);
                     Thread.sleep(10 * 1000);
                 }catch(InterruptedException e){
                     e.printStackTrace();
                 }
             }
             if(index+batch < vehicleTotalNum){
                 pool.execute(new WriteToParquetThread(index,false));
             }else{
                 pool.execute(new WriteToParquetThread(index,true));
             }

             index = index+batch;
         }
    }


    private void hbaseConnClose(){
         if(connection != null){
             try {
                 connection.close();
             }catch(IOException e){
                 e.printStackTrace();
             }
         }
    }

    public static void main(String[] args) {
        ParquetWriteDemo demo = null;
        try {
            demo = new ParquetWriteDemo();
            long startTimestamp = System.currentTimeMillis();
            demo.start();
            try {
                boolean terminated = false;
                do {
                    // wait until the pool has terminated
                    terminated = pool.awaitTermination(30, TimeUnit.SECONDS);
                    System.out.println("the active task num is :"+pool.getActiveCount());
                } while (!terminated);
            }catch(InterruptedException e){
                pool.shutdownNow();
            }
            System.out.println("the total cost time is :" + (System.currentTimeMillis() - startTimestamp));

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(demo != null){
                demo.hbaseConnClose();
            }
            System.out.println("the isException is :"+isException.get());
        }
    }
}


