package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortByFrequencyMapper extends Mapper<Object, Text, LongWritable, Text> {
    private Text word = new Text();
    private LongWritable frequency = new LongWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            word.set(parts[0]);
            frequency.set(Integer.parseInt(parts[1]));
            context.write(frequency, word);
        }
    }
}
