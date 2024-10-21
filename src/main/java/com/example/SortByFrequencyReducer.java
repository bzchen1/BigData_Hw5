
package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortByFrequencyReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
    private int count = 0;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            if (count < 100) {
                // 创建输出格式为 "index stock count"
                String outputKey = (count + 1) + "\t" + val.toString();
                context.write(new Text(outputKey), key);
                count++;
            } else {
                return;
            }
        }
    }
}
