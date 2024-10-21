package com.example;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class DescendingLongWritableComparator extends WritableComparator {

    public DescendingLongWritableComparator() {
        super(LongWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        LongWritable int1 = (LongWritable) a;
        LongWritable int2 = (LongWritable) b;
        // 降序排列
        return -int1.compareTo(int2);
    }
}
