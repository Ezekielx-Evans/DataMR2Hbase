import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// WordCountMapper 类继承自 Hadoop 的 Mapper 类
// 泛型参数说明：输入键类型为 LongWritable（行偏移量）+ 输入值类型为 Text（一行文本）；输出键类型为 Text（单词），输出值类型为 IntWritable（单词出现次数）
public class ProductKeywordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text wordText = new Text();

    // key：当前行在输入文件中的字节偏移量；value：当前行的文本内容；context：上下文，用于输出中间结果<单词, 1>
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

        // 把一行转换成字符串
        String line = value.toString();

        // 按特殊分隔符 \u0001 切分
        String[] fields = line.split("\u0001");

        System.out.println("title=" + fields[1]);

        // 确保有 2 列
        if (fields.length > 1) {

            String title = fields[1];

            // 按空格分词（多个空格也能处理），"\\s+"为正则表达式
            String[] words = title.split("\\s+");

            // 遍历每个单词
            for (String word : words) {
                // 忽略空字符串（例如多个连续空格可能造成空单词）
                if (!word.isEmpty()) {
                    // 输出 <单词, 1>，表示该单词出现一次
                    wordText.set(word);
                    context.write(wordText, one);
                }
            }

        }
    }
}