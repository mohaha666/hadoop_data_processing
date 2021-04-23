package com.my.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class LogIpCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar路径
        job.setJarByClass(LogIpCountSortDriver.class);
        // 关联map，reduce
        job.setMapperClass(LogIpCountSortMapper.class);
        job.setReducerClass(LogIpCountSortReducer.class);
        // 设置map输出类型
        job.setMapOutputKeyClass(LogBean.class);
        job.setMapOutputValueClass(Text.class);
        // 设置output输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LogBean.class);
        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 关联分区，设置分区数量
        job.setPartitionerClass(LogIpCountSortPartition.class);
        job.setNumReduceTasks(2);

        // 提交
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
