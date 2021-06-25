package com.my.mr.wordcount.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 需要获取到数据是从哪个文件中来的，map端给每条数据的来源打上标签
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    Text k = new Text();
    TableBean v = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (name.startsWith("access")) {
            // 日志文件中的内容
            String[] line = value.toString().split(" ");

            v.setIp(line[0]);
            v.setDate(line[3].replace("[", ""));
            v.setUrl(line[6]);
            if (line.length >= 10 && StringUtils.isNumeric(line[9])) {
                v.setFlow(Long.parseLong(line[9]));
            }else{
                v.setFlow(0L);
            }
            v.setUserId("");
            v.setFlag("access");
        } else {
            // 用户文件中的内容
            String[] line = value.toString().split("\t");

            v.setDate("");
            v.setUrl("");
            v.setFlow(0L);
            v.setFlag("user");
            v.setIp(line[0]);
            v.setUserId(line[1]);
        }
        k.set(v.getIp());
        context.write(k, v);
    }
}
