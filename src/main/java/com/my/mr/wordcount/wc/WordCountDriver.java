package com.my.mr.wordcount.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1 job
 * 2 jar
 * 3 mapper reducer
 * 4 data type
 * 5 in out
 * 6 submit
 */
public class WordCountDriver {
    // 注意导包，使用mapreduce下的包，mapred的都是过时的包
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar路径，采用setJarByClass，不使用setJar，把路径写死
        job.setJarByClass(WordCountDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置mapper的输入数据kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 5 设置输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 使用	CombineTextInputFormat，如果不设置fileinputformat,默认使用textinputformat
        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));




        // 优化项：combiner
        job.setCombinerClass(WordCountCombiner.class);



        // 7 提交job
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
