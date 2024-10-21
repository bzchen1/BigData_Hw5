package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, LongWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将每一行按制表符拆分
        String[] parts = value.toString().split("\t");

        // 确保行被正确解析为两个部分（股票代码 和 出现次数）
        if (parts.length == 2) {
            try {
                String stockCode = parts[0].trim();
                long count = Long.parseLong(parts[1].trim());

                // 输出出现次数作为 key，股票代码作为 value
                context.write(new LongWritable(count), new Text(stockCode));
            } catch (NumberFormatException e) {
                // 如果计数不是有效的整数，则输出错误日志
                System.err.println("解析错误，无法将计数转换为整数：" + parts[1]);
            }
        } else {
            // 输出错误日志，提示行解析失败
            System.err.println("解析错误，行未被正确分割：" + value.toString());
        }
    }
}
