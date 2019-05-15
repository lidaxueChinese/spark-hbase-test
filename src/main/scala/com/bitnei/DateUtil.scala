package com.bitnei

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by lidaxue@bitnei.cn on 2019/5/10.
  */
object DateUtil {

     val dayTimeStamp = 24*3600*1000L

     def getFilterDatePrefix(executeDate:String): (String,String) ={
         val sdf = new SimpleDateFormat("yyyyMMdd")
         val date = sdf.parse(executeDate).getTime
         val minPrefix = date.toString.substring(0,5)
         val maxPrefix = minPrefix.toInt+1
       (minPrefix,maxPrefix.toString)
     }

    def getMinAndMaxTimestamp(executeDate:String):(java.lang.Long,java.lang.Long) = {
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val startDateTimestamp = sdf.parse(executeDate).getTime
      val endDateTimestamp = new Date(startDateTimestamp+dayTimeStamp).getTime
      (startDateTimestamp,endDateTimestamp)
    }



}
