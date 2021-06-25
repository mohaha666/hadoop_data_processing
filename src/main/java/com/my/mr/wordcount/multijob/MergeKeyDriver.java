package com.my.mr.wordcount.multijob;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MergeKeyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // job
        Job job = Job.getInstance();
        // jar
        job.setJarByClass(MergeKeyDriver.class);
        // 关联mapper，reducer
        job.setMapperClass(MergeKeyMapper.class);
        job.setReducerClass(MergeKeyReducer.class);
        // 设置mapper的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 设置output的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
