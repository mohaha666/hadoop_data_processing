package com.my.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class IPPartitioner extends Partitioner<Text, IntWritable> {
    public int getPartition(Text k, IntWritable v, int numPartitions) {
        String key = k.toString();
        String[] split = key.split("\\.");

        // 前两位相同的key放在同一个文件中
        String head = "";
//        if(split.length>=2){
            head += split[0]+split[1];
//        }else{
//            head = key;
//        }

        return (head.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
