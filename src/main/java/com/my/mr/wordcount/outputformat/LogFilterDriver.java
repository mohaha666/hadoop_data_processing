package com.my.mr.wordcount.outputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogFilterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 获取jar路径
        job.setJarByClass(LogFilterDriver.class);
        // 关联map，reduce
        job.setMapperClass(LogFilterMapper.class);
        // 设置map输出类型
        job.setReducerClass(LogFilterReducer.class);
        // 设置output输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 虽然有LogFilterOutputFormat了，但是需要有一个_SUCCESS标记文件，因此此处还需要设置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置自定义OutputFormat
        job.setOutputFormatClass(LogFilterOutputFormat.class);
        // 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
