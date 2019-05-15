package com.bitnei;

import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Created by lidaxue@bitnei.cn on 2019/5/12.
 */
public class HbaseRowkeyFilter {

    public static FuzzyRowFilter getFuzzyRowkeyFilter(String minPrefix,String maxPrefix){
        FuzzyRowFilter rowFilter = new FuzzyRowFilter(java.util.Arrays.asList(
                new Pair<byte[],byte[]>(
                        Bytes.toBytes("????????????????????????????????_"+minPrefix+"????????"),
                        new byte[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,1,1,1,1,1,1,1,1}
                ),
                new Pair<byte[], byte[]>(
                        Bytes.toBytes("????????????????????????????????_"+maxPrefix+"????????"),
                        new byte[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,1,1,1,1,1,1,1,1}
                )


        ));
         return rowFilter;
    }
}
