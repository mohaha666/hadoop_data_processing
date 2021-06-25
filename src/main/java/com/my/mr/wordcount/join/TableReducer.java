package com.my.mr.wordcount.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        List<TableBean> data = new ArrayList<TableBean>();

        String userId = "";

        for(TableBean t : values){
            if("user".equals(t.getFlag())){
                // 用户表，通过它可获取到ip相对应的用户id，这个数据不记录在最后的输出结果中，仅仅用于映射
                userId = t.getUserId();
            }else{
                // 详细数据表
                TableBean dest = new TableBean();
                try {
                    // 必须要拷贝，因为t这时候存储的是一个引用，如果直接add，那么data中存储的永远是最后一个元素
                    BeanUtils.copyProperties(dest, t);
                    data.add(dest);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for(TableBean t : data){
            t.setUserId(userId);
            context.write(t, NullWritable.get());
        }
    }
}
