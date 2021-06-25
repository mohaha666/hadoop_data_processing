package com.my.mr.wordcount.multijob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class MergeKeyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    String path = "";
    Text k = new Text();
    LongWritable v = new LongWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();

        System.out.println(inputSplit.getPath());
        path = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");

        for(String word : words){
            k.set(word+"---"+path);
            context.write(k, v);
        }
    }
}
