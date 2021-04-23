package com.my.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable>{

    FileSplit split;
    Configuration conf;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        conf = context.getConfiguration();

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isProgress){
        // 将切片中的内容写出
            byte[] buf = new byte[(int) split.getLength()];
            // 根据切片获取fs
            Path path = this.split.getPath();
            FileSystem fs = path.getFileSystem(conf);

            // 获取输入流
            FSDataInputStream fis = fs.open(path);

            // 拷贝
            IOUtils.readFully(fis, buf, 0, buf.length);

            // 封装v
            v.set(buf, 0, buf.length);

            // 封装k
            k.set(path.toString());

            // 关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;
            return true;
        }

        // 如果已经读过了那就停止循环不再读取
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
