package com.my.mr.wordcount.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    private String ip;
    private String date;
    private String url;
    private Long flow;
    private String userId;
    private String flag;    //表的标记



    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeUTF(date);
        out.writeUTF(url);
        out.writeLong(flow);
        out.writeUTF(userId);
        out.writeUTF(flag);
    }

    public void readFields(DataInput in) throws IOException {
        ip = in.readUTF();
        date = in.readUTF();
        url = in.readUTF();
        flow = in.readLong();
        userId = in.readUTF();
        flag = in.readUTF();
    }

    @Override
    public String toString() {
        return "ip='" + ip + '\'' +
                ", date='" + date + '\'' +
                ", url='" + url + '\'' +
                ", flow=" + flow +
                ", userId='" + userId + '\'' +
                ", flag='" + flag + '\'';
    }

    public TableBean() {
        super();
    }

    public TableBean(String ip, String date, String url, Long flow, String userId, String flag) {
        super();
        this.ip = ip;
        this.date = date;
        this.url = url;
        this.flow = flow;
        this.userId = userId;
        this.flag = flag;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getFlow() {
        return flow;
    }

    public void setFlow(Long flow) {
        this.flow = flow;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
