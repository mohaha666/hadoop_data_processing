package com.my.mr.wordcount.groupcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IPFlowBean implements WritableComparable<IPFlowBean> {
    private String ip;
    private Long flow;


    public int compareTo(IPFlowBean ipFlowBean) {
        if(ipFlowBean.ip.equals(ip)){
            if(flow > ipFlowBean.flow){
                return -1;
            }else if(flow < ipFlowBean.flow){
                return 1;
            }else{
                return 0;
            }
        }else{
            return ip.compareTo(ipFlowBean.ip);
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeLong(flow);
    }

    public void readFields(DataInput in) throws IOException {
        this.ip = in.readUTF();
        this.flow = in.readLong();
    }

    public IPFlowBean() {
        super();
    }

    public IPFlowBean(String ip, Long flow) {
        this.ip = ip;
        this.flow = flow;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getFlow() {
        return flow;
    }

    public void setFlow(Long flow) {
        this.flow = flow;
    }

    @Override
    public String toString() {
        return ip + '\t' + flow;
    }
}
