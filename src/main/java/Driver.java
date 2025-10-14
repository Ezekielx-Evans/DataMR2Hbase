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

        //================================= 参数检查 =============================================
        // 现在要求输入 <JobType> <input path> <output path>
        if (args.length != 3) {
            System.err.println("Usage: Driver <JobType: ProductKeywordCount|ActionStatistics> <input path> <output path>");
            System.exit(-1);
        }

        // 读取参数
        String jobType = args[0];   // Job 类型
        String inputPath = args[1]; // 输入路径
        String outputPath = args[2]; // 输出路径

        HbaseConnection hbaseConnection = new HbaseConnection();


        //================================= Product Keyword Count =============================================
        if ("ProductKeywordCount".equalsIgnoreCase(jobType)) {
            // 词频统计，需要输入 <input path> <output path>
            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "Product Keyword Count");

            // 设置主类，用于打包 JAR 时指定入口点
            job1.setJarByClass(Driver.class);

            // 设置 Mapper、Reducer
            job1.setMapperClass(ProductKeywordCountMR.ProductKeywordCountMapper.class);
            job1.setReducerClass(ProductKeywordCountMR.ProductKeywordCountReducer.class);

            // 设置输出 key/value 类型
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            // 输入输出路径
            FileInputFormat.addInputPath(job1, new Path(inputPath));
            FileOutputFormat.setOutputPath(job1, new Path(outputPath));

            // 提交任务并等待完成
            boolean completed1 = job1.waitForCompletion(true);

            // 输出结果路径
            String partFile = new File(outputPath, "part-r-00000").getPath();
            // 将结果储存起来
            hbaseConnection.StoreKeywordCount(partFile);

            // 打印执行结果
            if (completed1) {
                System.out.println("商品关键字统计成功！");
            } else {
                System.out.println("商品关键字统计失败！");
            }
        }

        //==================================== ActionStatistics ===============================================
        else if ("ActionStatistics".equalsIgnoreCase(jobType)) {
            // 用户行为统计，需要输入 <input path> <output path>
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Action Statistics");

            job2.setJarByClass(Driver.class);

            // 设置 Mapper、Reducer
            job2.setMapperClass(ActionStatisticsMR.ActionStatisticsMapper.class);
            job2.setReducerClass(ActionStatisticsMR.ActionStatisticsReducer.class);

            // 设置输出 key/value 类型
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            // 输入输出路径
            FileInputFormat.addInputPath(job2, new Path(inputPath));
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            // 提交任务并等待完成
            boolean completed2 = job2.waitForCompletion(true);

            // 输出结果路径
            String partFile2 = new File(outputPath, "part-r-00000").getPath();
            hbaseConnection.StoreActionStatistics(partFile2);

            // 打印执行结果
            if (completed2) {
                System.out.println("用户行为统计成功！");
            } else {
                System.out.println("用户行为统计失败！");
            }
        }

        //==================================== UserDailyActivity ===============================================
        else if ("UserDailyActivity".equalsIgnoreCase(jobType)) {
            // 用户日活统计，需要输入 <input path> <output path>
            Configuration conf3 = new Configuration();
            Job job3 = Job.getInstance(conf3, "User Daily Activity");

            job3.setJarByClass(Driver.class);

            // 设置 Mapper、Reducer
            job3.setMapperClass(UserDailyActivityMR.UserDailyActivityMapper.class);
            job3.setReducerClass(UserDailyActivityMR.UserDailyActivityReducer.class);

            // 设置输出 key/value 类型
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);

            // 输入输出路径
            FileInputFormat.addInputPath(job3, new Path(inputPath));
            FileOutputFormat.setOutputPath(job3, new Path(outputPath));

            // 提交任务并等待完成
            boolean completed3 = job3.waitForCompletion(true);

            // 输出结果路径
            String partFile3 = new File(outputPath, "part-r-00000").getPath();
            hbaseConnection.StoreUserDailyActivity(partFile3);

            // 打印执行结果
            if (completed3) {
                System.out.println("用户日活统计成功！");
            } else {
                System.out.println("用户日活统计失败！");
            }
        }

        //==================================== 错误处理 ===============================================
        else {
            System.err.println("Unknown JobType: " + jobType);
            System.exit(-1);
        }

    }
}
