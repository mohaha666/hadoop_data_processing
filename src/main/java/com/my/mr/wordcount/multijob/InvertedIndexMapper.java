package com.my.mr.wordcount.multijob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable,Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // hello---a.txt	3
        String[] words = value.toString().split("---");

        k.set(words[0]);

        String[] post_num = words[1].split("\t");

        v.set(post_num[0]+"-->"+post_num[1]);

        context.write(k, v);
    }
}
