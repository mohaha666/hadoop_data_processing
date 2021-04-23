package com.my.mr.wordcount.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 计数mapper
 * KEYIN：输入的key，即偏移量
 * VALUEIN,：输入的值，即内容
 * KEYOUT：输出的key
 * VALUEOUT：输出的值
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text k = new Text();
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] content = line.split(" ");
        for (String word: content) {
            k.set(word);
            // context.write将内容写入环形缓冲区
            context.write(k, v);
        }
    }
}
