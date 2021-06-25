package com.my.mr.wordcount.cleanlog;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String[] fields = value.toString().split(" ");

        // 长度大于11才算是比较丰满的数据
//        if(fields.length>=11){
//            if(fields.length>=9 && StringUtils.isNumeric(fields[8])){// 有状态信息，是正常的请求
//                String status = fields[8];
//                if(!status.equals("404") && !status.equals("500")){
//                    context.write(value,NullWritable.get());
//                }
//            }
//        }

        boolean result = parseLog(value.toString(), context);

        if(!result){
            return;
        }

        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");

        if(fields.length>=11){

            context.getCounter("map","true").increment(1);
            return true;
        }else{
            context.getCounter("map","false").increment(1);
            return false;
        }

    }
}
