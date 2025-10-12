import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 词频统计的 Reducer，将 <单词, 1> 合并，输出 <单词, 次数>
// WordCountReducer 类继承自 Hadoop 的 Reducer 类
// 泛型参数说明：输入键类型为 Text（单词），输入值类型为 IntWritable（一组表示次数的值）；输出键类型为 Text（单词），输出值类型为 IntWritable（总次数）
public class ProductKeywordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);

        // 输出 <关键词, 出现次数>
        context.write(key, result);
    }
}
