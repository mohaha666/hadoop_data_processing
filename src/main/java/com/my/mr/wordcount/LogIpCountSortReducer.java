package com.my.mr.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogIpCountSortReducer extends Reducer<LogBean, Text, Text, LogBean> {
    @Override
    protected void reduce(LogBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text v : values){
            context.write(v, key);
        }
    }
}
