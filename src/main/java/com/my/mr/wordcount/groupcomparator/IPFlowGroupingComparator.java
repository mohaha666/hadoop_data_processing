package com.my.mr.wordcount.groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IPFlowGroupingComparator extends WritableComparator {

    protected IPFlowGroupingComparator(){
        // createInstances参数要给定true
        super(IPFlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 只要ip相同，那么就认为是相同的key
        IPFlowBean a1 = (IPFlowBean) a;
        IPFlowBean b1 = (IPFlowBean) b;

        if(a1.getIp().equals(b1.getIp())){
            return 0;
        }else{
            return a1.getIp().compareTo(b1.getIp());
        }
//        return super.compare(a, b);
    }
}
