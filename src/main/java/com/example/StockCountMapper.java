package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;//处理与输入和输出操作相关的错误

public class StockCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 值为 1 的静态示例
    private final static IntWritable one = new IntWritable(1);
    // 保存股票代码
    private Text stock = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 确保行不是表头
        if (line.startsWith("index,headline,date,stock")) {
            return;
        }

        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); // 使用正则表达式处理包含逗号的字段

        try {
            // 提取股票代码（假设是第 4 列）
            String stockCode = parts[3].trim();
            if (!stockCode.isEmpty()) {
                stock.set(stockCode);
                context.write(stock, one); // 输出股票代码和计数 1
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            // 忽略列数不足的行
        }
    }
}
