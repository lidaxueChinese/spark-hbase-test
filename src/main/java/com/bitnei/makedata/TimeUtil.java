package com.bitnei.makedata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by lidaxue@bitnei.cn on 2019/5/13.
 */
public class TimeUtil {
      private final static long TIME_INTERVER = 170*1000L;
      private final static int NUM = 500;
      private final static long DAY_PERIOD = 24*3600*1000L;

      public static List<Long> getTimestamp(String execute) throws Exception{
          List<Long> list = new ArrayList<Long>(NUM);
         SimpleDateFormat sdf =  new SimpleDateFormat("yyyyMMdd");
         Date date = sdf.parse(execute);
         long startTimestamp = date.getTime();
         for(int i = 0;i<NUM;i++){
             startTimestamp = startTimestamp+TIME_INTERVER;
             list.add(startTimestamp);
         }
         return list;
      }

      public static List<String> getDateStr(String startDateStr,int days) throws Exception{
          List<String> dateStrList = new ArrayList<String>();
          SimpleDateFormat sdf =  new SimpleDateFormat("yyyyMMdd");
          Long startDate = sdf.parse(startDateStr).getTime();
          dateStrList.add(startDateStr);
          for(int i =1 ;i<days;i++){
              Date date = new Date(startDate+i*DAY_PERIOD);
              dateStrList.add(sdf.format(date));
          }
          return dateStrList;
      }

    public static void main(String[] args) throws Exception {
        /*List<String> list = getDateStr("20190420",30);
        for(String dateStr:list){
            System.out.println(dateStr);
        }*/

        List<Long> list = getTimestamp("20190513");
        int num = list.size();
        System.out.println(list.get(num-1));
    }
}
