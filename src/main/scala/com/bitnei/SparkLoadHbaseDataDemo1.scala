package com.bitnei

import java.util

import com.bitnei.model.RealInfoModel
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.Result

/**
  * Created by lidaxue@bitnei.cn on 2019/5/9.
  */
class SparkLoadHbaseDataDemo1(@transient private var sc:SparkContext,executeDate:String) extends Serializable{

  val sparkSession = SparkSession.builder().enableHiveSupport().config(sc.getConf).getOrCreate()


  def loadDataFromHbase(): Unit ={

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "cnqycspser01,cnqycspser02,cnqycspser03")
    hbaseConf.set("hbase.master", "cnqycspser01:60000")
    hbaseConf.setLong("hbase.rpc.timeout", 5 * 60 * 1000L)

    val timePeriod = DateUtil.getMinAndMaxTimestamp(executeDate)
    val startTimestamp = timePeriod._1
    val endTimestamp = timePeriod._2
    val hBaseContext = new HBaseContext(sc,hbaseConf)
    val scan = new Scan()
    scan.setCacheBlocks(false)
    scan.setCaching(50)
    val timestampPrefix = DateUtil.getFilterDatePrefix(executeDate)
    //fuzzy scan  HbaseRowkeyFilter.getFuzzyRowkeyFilter(timestampPrefix._1,timestampPrefix._2)
    // regex scan getRegexRowkeyFilter()
    scan.setFilter(HbaseRowkeyFilter.getFuzzyRowkeyFilter(timestampPrefix._1,timestampPrefix._2))

    val scanRdd = hBaseContext.hbaseRDD(TableName.valueOf("realinfo"),scan)
    val finalRdd = scanRdd.filter(f=>{
       val rowKey = Bytes.toString(f._1.copyBytes())
       getSpecialDayData(rowKey,startTimestamp,endTimestamp)
    }).map(r=>{
       generateRealInfoModelFromHbaseValue(r._2)
    })

    import sparkSession.implicits._
    finalRdd.toDF().write.mode(SaveMode.Overwrite).parquet(s"/tmp/spark/vehicle/result/term_result/${executeDate}")

  }

  def generateRealInfoModelFromHbaseValue(result:Result): RealInfoModel ={
     val cfMap = result.getFamilyMap(Bytes.toBytes("cf"))
     val VID = if(cfMap.containsKey(Bytes.toBytes("VID"))){
        Bytes.toString(cfMap.get(Bytes.toBytes("VID")))
     }else{
       "-1"
     }
    val str7615 =  if(cfMap.containsKey(Bytes.toBytes("7615"))){
      Bytes.toString(Bytes.toBytes("7615"))
    }else{
      "-1"
    }
    RealInfoModel(VID,str7615)

  }

  /*
    startTimeStamp:当天最小时间戳
    endTimeStamp : 当天下一天的最小时间戳
    即前闭后开
   */
  def getSpecialDayData(rowkey:String,startTimeStamp:java.lang.Long,endTimeStamp:java.lang.Long):Boolean = {
      val timeStamp = rowkey.split("_")(1).toLong
      if(timeStamp >= startTimeStamp && timeStamp < endTimeStamp){
        true
      }else{
        false
      }
  }

  def getRegexRowkeyFilter() : Filter = {
     val timestampPrefix = DateUtil.getFilterDatePrefix(executeDate)
     val regexKey = s"[\\dA-Fa-f]{32}_((${timestampPrefix._1}|${timestampPrefix._2})[\\d]{8}){1}"
     val regexStringComparator = new RegexStringComparator(regexKey)
     val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, regexStringComparator)
     rowFilter
  }


}


object SparkLoadHbaseDataDemo1 {
     def main(args:Array[String]): Unit ={
       if(args.length <1){
         println("the executeDate is missed")
         System.exit(-1)
       }

       val sparkConf = new SparkConf().setAppName("SparkLoadHbaseDemo1")
       val sc = new SparkContext(sparkConf)
       val executeDate = args(0)
       val obj = new SparkLoadHbaseDataDemo1(sc,executeDate)
       obj.loadDataFromHbase()
     }
}


