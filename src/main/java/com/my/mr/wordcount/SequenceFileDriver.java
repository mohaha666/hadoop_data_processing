package com.my.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置jar路径
        job.setJarByClass(SequenceFileDriver.class);

        // 3. 关联map和reduce
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 4. 设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 5. 设置output输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 设置自定义inputformat
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 8. 设置输出的outputformat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 9. 提交
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
