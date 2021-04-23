package com.my.mr.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区的输入就是map的输出
 */
public class LogIpCountSortPartition extends Partitioner<LogBean, Text> {
    public int getPartition(LogBean logBean, Text text, int numPartitions) {
        if(text.toString().startsWith("172")){
            return 0;
        }else{
            return 1;
        }
    }
}
