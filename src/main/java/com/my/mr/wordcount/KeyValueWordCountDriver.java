package com.my.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KeyValueWordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取job
        Configuration conf = new Configuration();
        // 设置keyvalue的分隔符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        Job job = Job.getInstance(conf);
        // 2. 设置jar路径
        job.setJarByClass(KeyValueWordCountDriver.class);
        // 3. 关联map，reduce
        job.setMapperClass(KeyValueWordCountMapper.class);
        job.setReducerClass(KeyValueWordCountReducer.class);
        // 4. 设置map的输出类型，结果集的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5. 设置输入数据的路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6. 设置输入格式inputformatclass为KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 7. 提交任务
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
