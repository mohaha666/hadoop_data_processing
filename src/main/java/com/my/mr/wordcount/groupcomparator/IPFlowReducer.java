package com.my.mr.wordcount.groupcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPFlowReducer extends Reducer<IPFlowBean, NullWritable, IPFlowBean, NullWritable> {
    @Override
    protected void reduce(IPFlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable v : values) {
            context.write(key, NullWritable.get());
        }
    }
}
