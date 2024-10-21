package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();// 配置信息
        // 创建一个job任务对象
        Job job = Job.getInstance(conf, "Stock Count");

        // 指定作业使用的 Map 和Reduce 类型
        job.setJarByClass(StockCountDriver.class);
        job.setMapperClass(StockCountMapper.class);
        job.setCombinerClass(StockCountReducer.class);
        job.setReducerClass(StockCountReducer.class);

        // 定义输出键、值的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入、输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 退出程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
