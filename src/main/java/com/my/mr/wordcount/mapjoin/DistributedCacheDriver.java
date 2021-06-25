package com.my.mr.wordcount.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar目录
        job.setJarByClass(DistributedCacheDriver.class);
        // 关联map
        job.setMapperClass(DistributedCacheMapper.class);
        // 设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置output输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置不要reduce端
        job.setNumReduceTasks(0);

        // todo 设置缓存表 注意路径以及调用的方法
        job.addCacheFile(new URI("file:///C://Users/jiamo/Documents/jm/tmp/hadoop/mapjoin/user.log"));
        // 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
