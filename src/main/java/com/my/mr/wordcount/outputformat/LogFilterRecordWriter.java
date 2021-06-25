package com.my.mr.wordcount.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogFilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosLocal;
    FSDataOutputStream fosOther;
    public LogFilterRecordWriter(TaskAttemptContext job) {

        try {
            // 获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());

            // 创建输出到local.log的输出流
            fosLocal = fs.create(new Path("C:\\Users\\jiamo\\Documents\\jm\\tmp\\hadoop\\outputformat\\realout\\local.log"));
            // 创建输出到other.log的输出流
            fosOther = fs.create(new Path("C:\\Users\\jiamo\\Documents\\jm\\tmp\\hadoop\\outputformat\\realout\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断key中是否存在172.24开头的ip，如果有，放入local，如果没有，放入other
        if(key.toString().startsWith("172.24")){
            fosLocal.write(key.toString().getBytes());
        }else{
            fosOther.write(key.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 底层会判空并且抓异常，不用自己取判断了
        IOUtils.closeStream(fosLocal);
        IOUtils.closeStream(fosOther);
    }
}
