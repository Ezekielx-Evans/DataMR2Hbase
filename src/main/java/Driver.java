import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {

        //================================= 参数检查 =============================================
        if (args.length != 3 || "-h".equalsIgnoreCase(args[0]) || "--help".equalsIgnoreCase(args[0])) {
            printHelp();
            System.exit(-1);
        }

        String jobType = args[0];    // Job 类型
        String inputPath = args[1];  // 输入路径 (HDFS)
        String outputPath = args[2]; // 输出路径 (HDFS)

        HbaseConnection hbaseConnection = new HbaseConnection();

        //================================= Product Keyword Count =============================================
        if ("ProductKeywordCount".equalsIgnoreCase(jobType)) {
            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "Product Keyword Count");

            job1.setJarByClass(Driver.class);
            job1.setMapperClass(ProductKeywordCountMR.ProductKeywordCountMapper.class);
            job1.setReducerClass(ProductKeywordCountMR.ProductKeywordCountReducer.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job1, new Path(inputPath));
            FileOutputFormat.setOutputPath(job1, new Path(outputPath));

            boolean completed1 = job1.waitForCompletion(true);
            String partFile = outputPath + "/part-r-00000";
            hbaseConnection.StoreKeywordCount(conf1, partFile);

            if (completed1) {
                System.out.println("商品关键字统计成功！");
            } else {
                System.out.println("商品关键字统计失败！");
            }
        }

        //==================================== ActionStatistics ===============================================
        else if ("ActionStatistics".equalsIgnoreCase(jobType)) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Action Statistics");

            job2.setJarByClass(Driver.class);
            job2.setMapperClass(ActionStatisticsMR.ActionStatisticsMapper.class);
            job2.setReducerClass(ActionStatisticsMR.ActionStatisticsReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(inputPath));
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            boolean completed2 = job2.waitForCompletion(true);
            String partFile2 = outputPath + "/part-r-00000";
            hbaseConnection.StoreActionStatistics(conf2, partFile2);

            if (completed2) {
                System.out.println("用户行为统计成功！");
            } else {
                System.out.println("用户行为统计失败！");
            }
        }

        //==================================== UserDailyActivity ===============================================
        else if ("UserDailyActivity".equalsIgnoreCase(jobType)) {
            Configuration conf3 = new Configuration();
            Job job3 = Job.getInstance(conf3, "User Daily Activity");

            job3.setJarByClass(Driver.class);
            job3.setMapperClass(UserDailyActivityMR.UserDailyActivityMapper.class);
            job3.setReducerClass(UserDailyActivityMR.UserDailyActivityReducer.class);

            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job3, new Path(inputPath));
            FileOutputFormat.setOutputPath(job3, new Path(outputPath));

            boolean completed3 = job3.waitForCompletion(true);
            String partFile3 = outputPath + "/part-r-00000";
            hbaseConnection.StoreUserDailyActivity(conf3, partFile3);

            if (completed3) {
                System.out.println("用户日活统计成功！");
            } else {
                System.out.println("用户日活统计失败！");
            }
        }

        //==================================== ItemConversion ===============================================
        else if ("ItemConversion".equalsIgnoreCase(jobType)) {
            Configuration conf4 = new Configuration();
            Job job4 = Job.getInstance(conf4, "Item Conversion Statistics");

            job4.setJarByClass(Driver.class);
            job4.setMapperClass(ItemConversionMR.ItemConversionMapper.class);
            job4.setReducerClass(ItemConversionMR.ItemConversionReducer.class);

            job4.setMapOutputKeyClass(Text.class);
            job4.setMapOutputValueClass(Text.class);

            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job4, new Path(inputPath));
            FileOutputFormat.setOutputPath(job4, new Path(outputPath));

            boolean completed4 = job4.waitForCompletion(true);
            String partFile4 = outputPath + "/part-r-00000";
            hbaseConnection.StoreItemConversion(conf4, partFile4);

            if (completed4) {
                System.out.println("商品转化率统计成功！");
            } else {
                System.out.println("商品转化率统计失败！");
            }
        }

        else {
            System.err.println("未知的作业类型: " + jobType);
            printHelp();
            System.exit(-1);
        }
    }

    private static void printHelp() {
        System.out.println("================================= Hadoop 作业运行帮助 =================================");
        System.out.println("用法:");
        System.out.println("    hadoop jar your-jar-file.jar Driver <作业类型> <输入路径> <输出路径>");
        System.out.println();
        System.out.println("参数说明:");
        System.out.println("    <作业类型> 可选值如下：");
        System.out.println("       ProductKeywordCount   - 商品关键字统计");
        System.out.println("       ActionStatistics      - 用户行为统计");
        System.out.println("       UserDailyActivity     - 用户日活统计");
        System.out.println("       ItemConversion        - 商品转化率统计");
        System.out.println();
        System.out.println("    <输入路径>   HDFS 输入数据的路径");
        System.out.println("    <输出路径>   HDFS 输出结果的路径");
        System.out.println();
        System.out.println("示例:");
        System.out.println("    hadoop jar analysis.jar Driver ProductKeywordCount /input/products /output/ProductKeywordCount");
        System.out.println("=====================================================================================");
    }
}
