package com.my.mr.wordcount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogBean implements WritableComparable<LogBean> {

    private String ip;
    private int cnt;


    // 比较
    public int compareTo(LogBean logBean) {
            if(cnt > logBean.cnt){
                return -1;
            }else if(cnt < logBean.cnt){
                return 1;
            }else{
                return 0;
            }
    }

    // 序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeInt(cnt);
    }

    // 反序列化
    public void readFields(DataInput in) throws IOException {
        // java.io.EOFException：使用in.readLine()的时候会报错
        ip = in.readUTF();

        System.out.println("readFields:"+ip);
        cnt = in.readInt();
    }

    public LogBean() {
        super();
    }

    public LogBean(String ip, int cnt) {
        super();
        this.ip = ip;
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return ip + '\t' + cnt;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
