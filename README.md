
# Task 1 统计上市公司股票代码出现次数
##### 1.StockCountMapper.java
重点在于如何切分字符串，提取stock。其中需要注意：
- **删去表头**
- **headline内含“，”，如果只用split(",")进行拆分，则某些行会切割出多列**
解决代码：
``` java 
// 除去表头
if (line.startsWith("index,headline,date,stock")) {
	return;
}
// 使用正则表达式处理包含逗号的字段
String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); 
```
##### 2.StockCountMapper.java
思路：对每一个val集合累和
```java
int sum = 0;
for (IntWritable val : values) {
	sum += val.get();
}
context.write(key, new IntWritable(sum));
```

##### 运行上面的程序后，发现**输出不是按照count降序排列，答案是依据stock的字母序排列**。

``` 部分结果
ZOES    28
ZPIN    14
ZQK     112
ZSL     66
ZSPH    34
ZTR     5
ZTS     220
ZU      62
ZUMZ    328
ZX      18
```

解决思路：**再写一个mapreduce程序实现降序排列。把第一个程序的<key,value>转为<value,key>处理**
##### 3.SortMapper.java
思路：将<key,value>转为<value,key>
``` java
String stockCode = parts[0].trim();
int count = Integer.parseInt(parts[1].trim());
// 输出出现次数作为 key，股票代码作为 value
context.write(new IntWritable(count), new Text(stockCode));
```
##### 4.SortReduce.java
思路：使用一个`index`作为全局变量，为每行的结果前面加上`index`。
```java
public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private int index = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        // 初始化索引为 1
        index = 1;
    }

    private IntWritable v = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            // 创建输出格式为 "index stock count"
            String outputValue = index + "\t" + val.toString();
            v.set(key.get());
            context.write(new Text(outputValue), v);
            // 递增索引
            index++;
        }
    }
}
```
##### 5.DescendingIntWritableComparator.java
实现升序。内部重构compare函数。
```java
 @Override
    public int compare(Object a, Object b) {
        IntWritable int1 = (IntWritable) a;
        IntWritable int2 = (IntWritable) b;
        // 降序排列
        return -int1.compareTo(int2);
    }
```


# Task 2 统计热点新闻标题前100个⾼频单词
##### 1.WordCountWithStopWordsMapper.java
处理数据集，提取`headline`，按照标点符号、空格等切割出word，然后舍弃部分不合法word，如全数字、单个字母、含数字的词(eg : $q1$)。同时处理stop_word，用于取舍word。输出<单词，1>
``` java
// 在 setup 方法中 提取stop_word
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
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
  
//map中：
//分解每行
String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
// 将 headline 转为小写，并按非字母字符拆分成单词
String[] words = headline.toLowerCase().split("\\W+");
// 忽略数字、单个字母、停用词、包含数字的词
if (w.isEmpty() || w.length() == 1 || stopWords.contains(w) || w.matches(".*\\d.*")) {
                    continue;
                  }
```
##### 2.WordCountWithStopWordsReducer.java
将<单词,<1,1,...>>转成  <单词，次数>
##### 3.SortByFrequencyMapper.java
和SortMapper.java一样，将<单词，次数>转成<次数，单词>
```java
//map内
String[] parts = value.toString().split("\t");
if (parts.length == 2) {
	word.set(parts[0]);
	frequency.set(Integer.parseInt(parts[1]));
	context.write(frequency, word);
}
```
##### 4.随后调用DescendingIntWritableComparator.java实现降序排列
##### 5.SortByFrequencyReducer.java

输出count前100的单词
```java
//reduce内
 for (Text val : values) {
	if (count < 100) { //全局变量，初始值为0
		// 创建输出格式为 "index stock count"
		String outputKey = (count + 1) + "\t" + val.toString();
		context.write(new Text(outputKey), key);
		count++;
	} else {
		return;
	}
}
```


# 程序运行结果
运行代码
```java
//计算stock 数目
hadoop jar target/stock_count.jar com.example.StockCountDriver /hw/hw5/input /hw/hw5/output1
//降序排列
hadoop jar target/stock_count.jar com.example.SortDriver /hw/hw5/output1 /hw/hw5/output2
//统计headline中各个词语数目
hadoop jar target/stock_count.jar com.example.WordCountWithStopWordsDriver /hw/hw5/input /hw/hw5/output3
//降序输出前100个
hadoop jar target/stock_count.jar com.example.SortByFrequencyDriver /hw/hw5/output3 /hw/hw5/output4
```
web截图
![[./hw5-1.png]]

# 纠错与改进
- 没有合适的debug方法，只能通过上传hdfs系统运行才能看到结果。
- 下次可以尝试通过日志来检查错误。