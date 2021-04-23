package com.my.mr.wordcount.groupcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IPFlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar目录
        job.setJarByClass(IPFlowDriver.class);
        // 关联map，reduce
        job.setMapperClass(IPFlowMapper.class);
        job.setReducerClass(IPFlowReducer.class);
        // 设置map输出类型
        job.setMapOutputKeyClass(IPFlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置output输出类型
        job.setOutputKeyClass(IPFlowBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 优化：设置GroupingComparator，让reduce按照自定义方式
        job.setGroupingComparatorClass(IPFlowGroupingComparator.class);

        // 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
