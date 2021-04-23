package com.my.mr.wordcount.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输入：（hello，1）
 * 输出：（hello，3）
 */
public class WordCountReducer extends Reducer<Text, LongWritable,Text, LongWritable> {
    LongWritable v = new LongWritable();
    Long total = new Long(0);
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        total = 0L;
        // 求和
        for (LongWritable v: values) {
            total += v.get();
        }
        // 写出
        v.set(total);
        context.write(key, v);
    }
}
