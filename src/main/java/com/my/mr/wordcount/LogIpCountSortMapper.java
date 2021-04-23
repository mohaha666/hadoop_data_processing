package com.my.mr.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class LogIpCountSortMapper extends Mapper<LongWritable, Text, LogBean, Text> {
    LogBean k = new LogBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取数据
        String[] info = value.toString().split("\t");

        if(info.length>=2){
            k.setIp(info[0]);
            k.setCnt(Integer.valueOf(info[1]));
        }else{
            k.setIp(info[0]);
            k.setCnt(0);
        }
        System.out.println(value.toString());
        System.out.println(Arrays.toString(info));



        v.set(info[0]);
        context.write(k, v);
    }
}
