package com.my.mr.wordcount.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import sun.reflect.misc.ReflectUtil;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        compress("C:\\Users\\jiamo\\Documents\\jm\\tmp\\hadoop\\compress\\access.log",
//                "org.apache.hadoop.io.compress.BZip2Codec");
//        compress("C:\\Users\\jiamo\\Documents\\jm\\tmp\\hadoop\\compress\\access.log",
//                "org.apache.hadoop.io.compress.GzipCodec");
//        compress("C:\\Users\\jiamo\\Documents\\jm\\tmp\\hadoop\\compress\\access.log",
//                "org.apache.hadoop.io.compress.DefaultCodec");

        decompress("C:\\Users\\jiamo\\Documents\\jm\\tmp\\hadoop\\compress\\access.log.gz");
    }

    private static void decompress(String filename) throws IOException {
        // 验证压缩方式
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path((filename)));

        if(codec == null){
            System.out.println("不支持该压缩方式");
            return;
        }

        // 输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        CompressionInputStream cis = codec.createInputStream(fis);

        // 输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decode"));

        // 对拷
        IOUtils.copyBytes(cis, fos, 1024*1024*5, false);

        // 关闭
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }

    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {
        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));


        Class<?> classCodec = Class.forName(method);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(classCodec, new Configuration());

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(filename + codec.getDefaultExtension());
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 流对拷
        IOUtils.copyBytes(fis, cos, 1024*1024*5, false);

        // 关闭流
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
