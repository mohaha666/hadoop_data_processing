package com.my.mr.wordcount.groupcomparator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPFlowMapper extends Mapper<LongWritable, Text, IPFlowBean, NullWritable> {
    IPFlowBean bean = new IPFlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split(" ");

        bean.setIp(info[0]);
        if(info.length>=10 && StringUtils.isNumeric(info[9])){
            bean.setFlow(Long.valueOf(info[9]));
        }else{
            bean.setFlow(0L);
        }
        context.write(bean, NullWritable.get());
    }
}
