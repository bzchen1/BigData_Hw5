package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
    private int index = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 初始化索引为 1
        index = 1;
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            // 创建输出格式为 "index stock count"
            String outputValue = index + "\t" + val.toString();
            context.write(new Text(outputValue), key);
            // 递增索引
            index++;
        }
    }
}
