package com.my.mr.wordcount.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Map<String, String> userMap = new HashMap<String, String>();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 从缓存中获取到小表内容
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        // 将小表读出来
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        // 循环读取流数据
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fields = line.split("\t");
            userMap.put(fields[0], fields[1]);
        }

        // 关闭流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");

        String ip = fields[0];
        String userId = userMap.get(ip);

        String url = "";
        if(fields.length>=7) {
            url = fields[6];
        }

        String res = ip +" "+userId+" "+url;
        k.set(res);

        context.write(k, NullWritable.get());
    }
}
