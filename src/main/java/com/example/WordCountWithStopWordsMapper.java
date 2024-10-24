package com.example;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class WordCountWithStopWordsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> stopWords = new HashSet<>();
    private CSVParser csvParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 初始化 CSVParser
        csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withIgnoreQuotations(true) // 忽略引号
                .build();
        URI[] stopWordFiles = context.getCacheFiles();
        if (stopWordFiles != null && stopWordFiles.length > 0) {
            for (URI stopWordFile : stopWordFiles) {
                Path path = new Path(stopWordFile);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("index,headline,date,stock")) {
            return; // 过滤掉表头
        }
        String[] parts = csvParser.parseLine(line);
        if (parts != null && parts.length > 2) {
            String headline = parts[1].trim(); // 提取 headline 列
            // 将 headline 转为小写，并按非字母字符拆分成单词
            String cleaned = headline.toLowerCase().replaceAll("[^a-zA-Z0-9\\s]", "");
            String[] words = cleaned.split("\\s+");
            for (String w : words) {
                // 忽略数字、单个字母和停用词
                if (w.isEmpty() || w.length() == 1 || stopWords.contains(w) || w.matches(".*\\d.*")) {
                    continue;
                }
                word.set(w);
                context.write(word, one);
            }
        }
    }
}
