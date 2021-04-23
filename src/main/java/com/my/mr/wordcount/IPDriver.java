package com.my.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IPDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取job
        Configuration conf = new Configuration();

        // 8. 设置输入格式中的分割方式
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        Job job = Job.getInstance(conf);
        // 2. 设置jar路径
        job.setJarByClass(IPDriver.class);
        // 3. 关联map，reduce
        job.setMapperClass(KeyValueWordCountMapper.class);
        job.setReducerClass(KeyValueWordCountReducer.class);
        // 4. 设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置output输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 设置输入格式，keyvaluefileinputformat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 9. 设置分区规则
        job.setPartitionerClass(IPPartitioner.class);

        // 10. 设置分区数
        job.setNumReduceTasks(10);

        // 10. 提交
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
