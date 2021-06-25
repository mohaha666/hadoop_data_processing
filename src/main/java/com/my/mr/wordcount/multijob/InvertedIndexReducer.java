package com.my.mr.wordcount.multijob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer res = new StringBuffer();
        for(Text v : values){
            res.append(v);
            res.append(" ");
        }

        v.set(res.toString());
        context.write(key, v);
    }
}
