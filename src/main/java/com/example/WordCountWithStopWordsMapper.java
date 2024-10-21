package com.example;

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

    // 在 setup 方法中
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if (parts.length >= 2) {
            String headline = parts[1];

            // 将 headline 转为小写，并按非字母字符拆分成单词
            String[] words = headline.toLowerCase().split("\\W+");

            for (String w : words) {
                // 忽略数字、单个字母和停用词
                if (w.isEmpty() || w.length() == 1 || stopWords.contains(w) || w.matches(".*\\d.*")) {
                    continue;
                }
                if (!w.isEmpty() && !stopWords.contains(w)) {
                    word.set(w);
                    context.write(word, one);
                }
            }
        }
    }
}
