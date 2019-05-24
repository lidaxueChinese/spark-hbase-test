package com.bitnei

import com.bitnei.model.RealInfoModel
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
  * 通过fuzzy，构建每个小时构建数据
  * Created by lidaxue@bitnei.cn on 2019/5/9.
  */
class SparkLoadHbaseDataDemo1(@transient private var sc:SparkContext,executeDate:String) extends Serializable{

  val sparkSession = SparkSession.builder().enableHiveSupport().config(sc.getConf).getOrCreate()


  def loadDataFromHbase1(): Unit ={

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "cnqycspser01,cnqycspser02,cnqycspser03")
    hbaseConf.set("hbase.master", "cnqycspser01:60000")
    hbaseConf.setLong("hbase.rpc.timeout", 5 * 60 * 1000L)
    val schema = StructType(Array(StructField("VID",StringType,true),StructField("str7615",StringType,true)))
    val executeDateTimestamp = DateUtil.getTimestampFromDateStr(executeDate);
    val hBaseContext = new HBaseContext(sc,hbaseConf)
    val scan = new Scan()
    scan.setCacheBlocks(false)
    scan.setCaching(100)
    for(hour <- 0 until 2) {
      val timeIndexList = DateUtil.getTimestampList(executeDateTimestamp,hour).toList.map(value => new Integer(value)).asJava
      scan.setFilter(HbaseRowkeyFilter.getFuzzyRowkeyFilter(timeIndexList))

      val scanRdd = hBaseContext.hbaseRDD(TableName.valueOf("realinfo"), scan)
      val finalRdd = scanRdd.filter(f => {
        val rowKey = Bytes.toString(f._1.copyBytes())
        getSpecialDayData(rowKey,executeDateTimestamp,hour)
      }).map(r => {
        generateRowFromHbaseValue(r._2)
      })

      import sparkSession.implicits._
      //finalRdd.toDF().write.mode(SaveMode.Overwrite).parquet(s"/tmp/spark/vehicle/result/term_result/${executeDate}-1/${hour}")
      sparkSession.createDataFrame(finalRdd,schema).write.mode(SaveMode.Overwrite).parquet(s"/tmp/spark/vehicle/result/term_result/${executeDate}-1/${hour}")

    }
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

  def generateRowFromHbaseValue(result:Result): Row ={
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
    Row(VID,str7615)

  }

  /*
    startTimeStamp:当天最小时间戳
    endTimeStamp : 当天下一天的最小时间戳
    即前闭后开
   */
  def getSpecialDayData(rowkey:String,dayInitTimestamp:java.lang.Long, hour:Int):Boolean = {
    var timeStamp = 0L
    try{
      timeStamp = rowkey.split("_")(1).toLong
    }catch{
      case ex:NumberFormatException =>{
        println("the exception rowkey is :"+rowkey)
        false
      }

    }
    val startTimestamp  = dayInitTimestamp + hour* 3600000
    val endTimestamp = startTimestamp + 3600000
    if(timeStamp >= startTimestamp && timeStamp < endTimestamp){
      true
    }else{
      false
    }
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
       obj.loadDataFromHbase1()
     }
}


