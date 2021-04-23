package com.my.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueWordCountMapper extends Mapper<Text, Text, Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//        context.write(key, value);
        context.write(key, v);
    }
}
