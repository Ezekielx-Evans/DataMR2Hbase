import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("ERROR: ProductKeywordCountDriver <input path> <output path>");
            System.exit(-1);
        }
        HbaseConnection hbaseConnection = new HbaseConnection();

        //================================= Product Keyword Count =============================================
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Product Keyword Count");

        // 设置主类，用于打包 JAR 时指定入口点
        job1.setJarByClass(Driver.class);

        // 设置 Mapper、Reducer、Combiner
        job1.setMapperClass(ProductKeywordCountMapper.class);
        job1.setReducerClass(ProductKeywordCountReducer.class);

        // 设置输出 key/value 类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // 输入输出路径
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // 提交任务并等待完成
        boolean completed1 = job1.waitForCompletion(true);

        String partFile = new File(args[1], "part-r-00000").getPath();
        hbaseConnection.StoreKeywordValueCount(partFile);

        // 打印结果
        if (completed1) {
            System.out.println("商品关键字统计成功成功！");
        } else {
            System.out.println("商品关键字统计成功失败！");
        }

        //================================= Product Keyword Count =============================================



        //==================================== ActionStatistics ===============================================



    }
}
