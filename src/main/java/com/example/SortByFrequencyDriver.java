package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortByFrequencyDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort by frequency");

        job.setJarByClass(SortByFrequencyDriver.class);
        job.setMapperClass(SortByFrequencyMapper.class);
        job.setReducerClass(SortByFrequencyReducer.class);

        // 设置降序排序的比较器
        job.setSortComparatorClass(DescendingLongWritableComparator.class);

        job.setNumReduceTasks(1); // 确保所有数据进入一个 reducer

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
