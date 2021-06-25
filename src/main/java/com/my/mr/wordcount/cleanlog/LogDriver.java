package com.my.mr.wordcount.cleanlog;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // jar
        job.setJarByClass(LogDriver.class);
        // map
        job.setMapperClass(LogMapper.class);
        // map data type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // output data type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // fileinput fileoutput
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // set num reduce task : not reduce
        job.setNumReduceTasks(0);
        // submit
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
